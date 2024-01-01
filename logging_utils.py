import logging

# Initialisierung des Loggers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Erstellen des Handlers für die Konsole
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Formatierung der Ausgabe
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Hinzufügen des Handlers zum Logger
logger.addHandler(console_handler)

def get_logger():
    return logger
