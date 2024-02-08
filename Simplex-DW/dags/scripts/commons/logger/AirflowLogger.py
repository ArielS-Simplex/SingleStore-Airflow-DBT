from abc import ABC, abstractmethod

class AirflowLogger(ABC):
    """ Logger for MWAA Airflow """

    @abstractmethod
    def send_notification(self,
                          notification_type: str,
                          contex) -> None:
        pass