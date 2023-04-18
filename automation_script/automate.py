from automation_script import *

if __name__ == '__main__':
    automation_process = AutomationProcess(config_path='config.txt')
    automation_process.automate_by_rating_count()
