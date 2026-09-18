import importlib.util
import json
import os
from pathlib import Path
import tempfile
import socket
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('cloud', Path(__file__).parents[1] / 'backend/cloud_sync.py')
cloud = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cloud)
ACCOUNT = 'a' * 32


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        cloud.ROOT = Path(self.tmp.name)
        self.db = cloud.connect()
        with self.db:
            self.db.execute('INSERT INTO accounts(id,provider,label,identity,cursor) VALUES(?,?,?,?,?)', (ACCOUNT, 'drive', 'Test', 'owner', 'cursor'))

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def task(self, **changes):
        values = dict(id='b'*32, account=ACCOUNT, name='Test', local=str(cloud.ROOT / 'local'), remote='folder', direction='download', mount='volume', initialized=0)
        values.update(changes)
        Path(values['local']).mkdir(exist_ok=True)
        with self.db:
            self.db.execute('INSERT INTO tasks(id,account,name,local,remote,direction,mount,initialized) VALUES(:id,:account,:name,:local,:remote,:direction,:mount,:initialized)', values)
        return self.db.execute('SELECT * FROM tasks WHERE id=?', (values['id'],)).fetchone()

    def test_mount_loss_never_starts_transfer(self):
        task = self.task()
        with patch.object(cloud, 'mount_identity', return_value='other'), patch.object(cloud, 'rclone') as run:
            with self.assertRaisesRegex(cloud.Error, 'volume changed'):
                cloud.run_task(self.db, task)
            run.assert_not_called()

    def test_initial_bisync_rejects_two_populated_folders(self):
        task = self.task(direction='both')
        Path(task['local'], 'existing.txt').write_text('keep')
        with patch.object(cloud, 'mount_identity', return_value='volume'), patch.object(cloud, 'listing', return_value=('hash', [{'IsDir': False}])), patch.object(cloud, 'rclone') as run:
            with self.assertRaisesRegex(cloud.Error, 'one folder must be empty'):
                cloud.run_task(self.db, task)
            run.assert_not_called()

    def test_one_way_preserves_deleted_and_replaced_files(self):
        task = self.task()
        with patch.object(cloud, 'mount_identity', return_value='volume'), patch.object(cloud, 'listing', return_value=('hash', [])), patch.object(cloud, 'rclone') as run:
            cloud.run_task(self.db, task)
        args = run.call_args.args[1]
        self.assertEqual(args[0], 'copy')
        self.assertIn('--backup-dir', args)
        self.assertNotIn('--delete-excluded', args)
        self.assertEqual(self.db.execute('SELECT status FROM tasks').fetchone()[0], 'idle')

    def test_concurrent_cloud_change_is_not_acknowledged_as_synced(self):
        task = self.task()
        with patch.object(cloud, 'mount_identity', return_value='volume'), patch.object(cloud, 'listing', side_effect=[('before-transfer', []), ('concurrent-edit', [])]) as listing, patch.object(cloud, 'rclone'):
            cloud.run_task(self.db, task)
        self.assertEqual(listing.call_count, 1)
        self.assertEqual(self.db.execute('SELECT snapshot FROM tasks').fetchone()[0], 'before-transfer')

    def test_bisync_only_initializes_once(self):
        task = self.task(direction='both', initialized=1)
        with patch.object(cloud, 'mount_identity', return_value='volume'), patch.object(cloud, 'listing', return_value=('hash', [])), patch.object(cloud, 'rclone') as run:
            cloud.run_task(self.db, task)
        self.assertNotIn('--resync', run.call_args.args[1])

    def test_running_task_cannot_be_removed(self):
        task = self.task()
        with self.db:
            self.db.execute("UPDATE tasks SET status='running'")
        with self.assertRaisesRegex(cloud.Error, 'Pause'):
            cloud.action(self.db, {'action': 'task.remove', 'id': task['id']})
        self.db.rollback()
        cloud.action(self.db, {'action': 'task.pause', 'id': task['id']})
        self.assertEqual(self.db.execute('SELECT paused FROM tasks').fetchone()[0], 1)

    def test_account_reconnect_rejects_other_identity_and_restores_token(self):
        config = cloud.config_path(ACCOUNT)
        config.write_text('old authorization')
        with patch.object(cloud, 'api', return_value={'user': {'permissionId': 'other-owner'}}):
            with self.assertRaisesRegex(cloud.Error, 'same cloud account'):
                cloud.account_import(self.db, {'id': ACCOUNT, 'provider': 'drive', 'authorization': {'provider': 'drive', 'token': {'access_token': 'secret', 'refresh_token': 'secret'}}})
        self.assertEqual(config.read_text(), 'old authorization')
        self.assertEqual(config.stat().st_mode & 0o777, 0o600)

    def test_cloud_pages_preserve_all_changes(self):
        account = self.db.execute('SELECT * FROM accounts').fetchone()
        with patch.object(cloud, 'api', side_effect=[{'changes': [{'fileId': 'one'}], 'nextPageToken': 'next'}, {'changes': [], 'newStartPageToken': 'final'}]):
            self.assertEqual(cloud.poll(account), ('final', True))
        self.assertEqual(self.db.execute('SELECT cursor FROM accounts').fetchone()[0], 'cursor')

    def test_cloud_page_failure_does_not_advance_cursor(self):
        account = self.db.execute('SELECT * FROM accounts').fetchone()
        with patch.object(cloud, 'api', side_effect=[{'changes': [], 'nextPageToken': 'next'}, cloud.Error('offline')]):
            with self.assertRaises(cloud.Error):
                cloud.poll(account)
        self.assertEqual(self.db.execute('SELECT cursor FROM accounts').fetchone()[0], 'cursor')

    def test_overlap_rejected(self):
        self.task(local=str(cloud.ROOT / 'local'))
        with patch.object(cloud, 'local_folder', return_value=(str(cloud.ROOT / 'local/child'), 'volume')):
            with self.assertRaisesRegex(cloud.Error, 'overlap'):
                cloud.action(self.db, {'action':'task.create', 'account':ACCOUNT, 'local':'irrelevant', 'remote':'other', 'direction':'upload'})

    def test_inotify_observes_changes_without_polling_files(self):
        watcher = cloud.Watcher()
        try:
            watcher.add_tree(str(cloud.ROOT), 'task')
            Path(cloud.ROOT, 'file').write_text('change')
            affected, _ = watcher.drain()
            self.assertEqual(affected, {'task'})
            self.assertEqual(watcher.drain()[0], set())
        finally:
            watcher.close()

    def test_worker_rpc_and_shutdown(self):
        proc = subprocess.Popen([sys.executable, str(Path(__file__).parents[1] / 'backend/cloud_sync.py'), 'worker', str(cloud.ROOT)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            address = cloud.ROOT / 'worker.sock'
            for _ in range(100):
                if address.exists():
                    break
                if proc.poll() is not None:
                    _, error = proc.communicate()
                    self.fail(error.decode())
                time.sleep(0.02)
            self.assertEqual(address.stat().st_mode & 0o777, 0o600)
            for request, expected in [({'mode':'state'}, 'accounts'), ({'mode':'action','params':{'action':'invalid'}}, 'error')]:
                with socket.socket(socket.AF_UNIX) as client:
                    client.settimeout(2)
                    client.connect(str(address))
                    client.sendall(json.dumps(request).encode()+b'\n')
                    with client.makefile('rb') as response:
                        self.assertIn(expected, json.loads(response.readline()))
        finally:
            proc.terminate()
            _, error = proc.communicate(timeout=5)
        self.assertEqual(proc.returncode, 0, error.decode())

    def test_rejects_ambiguous_paths(self):
        for value in ('../outside', '/a/../b', 'a\nsecret'):
            with self.assertRaises(cloud.Error):
                cloud.remote_folder(value)
        with self.assertRaises(cloud.Error):
            cloud.config_path('../secret')


if __name__ == '__main__':
    unittest.main()
