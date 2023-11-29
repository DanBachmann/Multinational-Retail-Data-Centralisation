import logging
import sys
import threading
import process_manager
from process_manager import ProcessManager


logging.basicConfig(format="%(asctime)s: %(message)s",
                    level=logging.INFO, datefmt="%H:%M:%S")
logging.info("Multinational Retail Data Centralisation project starting")

process_manager = ProcessManager()
for arg in  sys.argv:
    if arg not in process_manager.valid_arguments_list and arg!='.':
        logging.error(f"invalid argument {arg} specified. valid arguments are \
                      {process_manager.valid_arguments_list}")
        exit()


thread_list = process_manager.initialise_threads(sys.argv)

for thread in thread_list:
    thread.start()

# wait for all threads to finish
for thread in thread_list:
    thread.join()

process_manager.finalise()

logging.info("all done")
