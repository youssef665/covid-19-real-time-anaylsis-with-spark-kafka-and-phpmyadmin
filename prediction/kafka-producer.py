from kafka import KafkaProducer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic = 'test'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Watchdog event handler to handle file events
class FileEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        # Read the newly created file and send it to Kafka topic
        with open(event.src_path, 'r') as file:
            lines = file.readlines()
            for line in lines:
                producer.send(topic, value=line.encode())

# Watchdog observer to monitor the directory for file events
observer = Observer()
event_handler = FileEventHandler()
directory_to_watch = './data/'  # Replace with your directory path

observer.schedule(event_handler, directory_to_watch)
observer.start()

try:
    while True:
        pass  # Keep running until interrupted
except KeyboardInterrupt:
    observer.stop()

observer.join()
