import copy
import glob
import threading

import dill
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent
from watchdog.observers import Observer


class SessionSync(FileSystemEventHandler):
    def __init__(self, path, namespace, interval=1):

        self.observer = None
        self.path = path
        self.interval = interval
        self.namespace = namespace

        self.__stop = False

    def run(self):
        print('run')
        observer = Observer()
        observer.schedule(self, self.path, recursive=True)
        observer.start()
        try:
            while observer.isAlive() and not self.__stop:
                observer.join(self.interval)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()

    def stop(self):
        self.__stop = True

    def __filter_out(self, path):
        name = path.split('/')[-1]
        if name.startswith('.'):
            return True
        if not name[-4:] == '.pkl':
            return False
        return False

    def __try_load(self, path):
        with open(path, 'rb') as file:
            try:
                key_to_val = dill.load(file)
                if not type(key_to_val) is dict:
                    return

                self.namespace.update(key_to_val)
            except Exception as e:
                pass

    def on_created(self, event):
        if type(event) is FileCreatedEvent:
            if self.__filter_out(event.src_path):
                return
            self.__try_load(event.src_path)

    def on_modified(self, event):
        if type(event) is FileModifiedEvent:
            if self.__filter_out(event.src_path):
                return
            self.__try_load(event.src_path)

    def write_once(self):
        for key, val in self.namespace.items():

            key_to_val = {key: val}

            compressed = None
            try:
                compressed = dill.dumps(key_to_val)
            except Exception as e:
                pass

            if compressed is not None:
                with open(self.path + key + '.pkl', 'wb') as file:
                    file.write(compressed)

    def read_once(self):
        filenames = glob.glob(self.path + "*.pkl")
        for filename in filenames:
            if self.__filter_out(filename):
                continue
            self.__try_load(filename)

    def read_threaded(self):

        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True  # Daemonize thread
        thread.start()  # Start the execution

    def stop(self):
        if self.observer is not None:
            self.observer.stop()
            self.observer = None

