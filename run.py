"""Running the etra ETL application"""
import logging
import logging.config
import yaml


def main():
    """
    Entry point to run the Xetra ETL job
    """

    #Parsing YAML file
    config_path = 'I:/Rasika/Data Engineering/xetra_project/xetra_1234/configs/xetra_report1_config.yml'
    config = yaml.safe_load(open(config_path))
    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is an info msg")


if __name__ == '__main__':
    main()