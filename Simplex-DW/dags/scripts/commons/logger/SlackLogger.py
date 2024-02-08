import uuid
import logging as log
from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from scripts.commons.logger.AirflowLogger import AirflowLogger
from scripts.commons.consts import Consts as cnst
from scripts.commons.postgresql.PostgreSQLManager import PostgreSQLManager as PSM
import requests
from slack_sdk import WebClient
import matplotlib.pyplot as plt
import numpy as np
from io import BytesIO

class SlackLogger(AirflowLogger):
    """ Slack logger/notifier for Airflow """

    def __init__(self,
                 environment: str,
                 channel_id: str
                 ):
        self.environment = environment
        self.channel_id = channel_id

    def send_notification(self, notification_type: str, context, config=None, channels: list = [],
                          log_to_db: bool = False) -> None:
        #  Send notification to slack channel & log_to_db"""
        exception = str(context['exception']).replace("'", "")
        log.info(f"_______ {exception}______")
        if exception == "Detected as zombie":
            return exit()

        if log_to_db is True:
            self.__log_to_db(context, config)
        if notification_type == cnst.ERROR:
            logo = ':red_circle: Task Failed.'
        elif notification_type == cnst.WARNING:
            logo = ':yellow_circle: Warning'
        elif notification_type == cnst.INFO:
            logo = ':grey_circle: Info'
        slack_msg = f"""
                    {logo} 
                    *Task*: {context.get('task_instance').task_id}  
                    *Dag*: {context.get('task_instance').dag_id} 
                    *Execution Time*: {context.get('execution_date')}  
                    *Log Url*: {context.get('task_instance').log_url} 
                    """
        channels.append(self.channel_id)
        log.info(channels)
        for channel in channels:
            log.info(f"Sending notification to channel {channel}")
            slack_webhook_token, slack_channel = self.__get_slack_conn_id(channel)
            failed_alert = SlackWebhookOperator(
                task_id=f'slack_notif_{uuid.uuid4()}',
                http_conn_id=slack_channel,
                webhook_token=slack_webhook_token,
                message=slack_msg)
            failed_alert.execute(context=context)

        return

    def __log_to_db(self, context, config, status='log_exception'):
        """ Write log to db_utils """
        PSM.insert_2_etl_logs(status=status, config_data=config, **context)

    def __get_slack_conn_id(self, channel_id: str) -> str:
        """ Get channel slack conn id by channel and environment type """
        if self.environment != cnst.PROD_ENV:
            return BaseHook.get_connection(cnst.PIPELINE_DEV_CHANNEL).password, cnst.PIPELINE_DEV_CHANNEL
        else:
            # retrieve relevant prod slack conn id
            return BaseHook.get_connection(channel_id).password, channel_id

    def send_general_notification(self, message_text: str) -> None:
        """ Send general notification to slack channel """
        slack_webhook_token, slack_channel = self.__get_slack_conn_id(self.channel_id)
        slack_url = 'https://hooks.slack.com/services' + slack_webhook_token

        payload = '{"text":"%s"}' % message_text
        response = requests.post(slack_url, data=payload)
        print("slack response: " + str(response))

    @staticmethod
    def send_notification_file(file, filename, channel_id, initial_comment='', message_text='test') -> None:
        """ Send notification to Slack channel with file ***webhook can't send files only token***"""
        slack_token = BaseHook.get_connection('slack_token').password
        client = WebClient(token=slack_token)
        response = client.files_upload_v2(
            channel=channel_id,
            initial_comment=initial_comment,
            filename=filename,
            file=file)

        print(response)

    @staticmethod
    def plot_table_notification(df_filter, grid_color, channel_id):
        row_headers = df_filter.iloc[:, 0].tolist()
        column_headers = df_filter.keys()[1:].tolist()  # all the columns except the filter column
        fig, ax = plt.subplots(figsize=(len(column_headers), len(row_headers)))
        ax.axis('off')
        table_data = df_filter.iloc[:, 1:]
        table_data = table_data.values.tolist()  # we want to get the df except the 'lp' column
        rcolors = np.full(len(row_headers), grid_color)
        ccolors = np.full(len(column_headers), grid_color)
        table = ax.table(cellText=table_data,
                         cellLoc='left', loc='center',
                         rowLabels=row_headers, rowColours=rcolors,
                         colLabels=column_headers, colColours=ccolors,
                         )
        table.auto_set_column_width(col=list(range(len(df_filter.columns))))
        # send plot to slack
        buffer = BytesIO()
        plt.savefig(buffer, format='jpeg', bbox_inches='tight', dpi=150)
        buffer.seek(0)
        image_bytes = buffer.read()
        SlackLogger.send_notification_file(file=image_bytes, filename='plot.png',
                                           initial_comment='Liquidity providers volume change',
                                           channel_id=channel_id)
