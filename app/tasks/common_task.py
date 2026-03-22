from app.celery.celery_app import celery_app
from app.logging.logger import setup_logger
from app.utils.wrapper_utils import log_task_run
from app.services.kafka_metric_service import KafkaMetricService
from app.services.filter_service import FilterService


logger = setup_logger(__name__)


@celery_app.task(
    name="app.tasks.common_task.collect_kafka_metrics",
    time_limit=300,
)
@log_task_run(
    task_name="collect_kafka_metrics",
    task_type="monitor",
)
def collect_kafka_metrics() -> None:
    """
    Collect Kafka cluster + topic metrics and store into ClickHouse.
    """
    logger.info("Start task: collect_kafka_metrics")

    try:
        service = KafkaMetricService()
        service.collect_all()

    except Exception as e:
        logger.exception(
            "Error in collect_kafka_metrics: %s",
            str(e),
        )
    logger.info("Finish task: collect_kafka_metrics")


@celery_app.task(
    name="app.tasks.common_task.refresh_target_market",
    time_limit=300,
)
@log_task_run(
    task_name="refresh_target_market",
    task_type="filter",
)
def refresh_target_market() -> None:
    """
    Refresh target market table using recent snapshot data only.
    """
    logger.info("Start task: refresh_target_market")

    try:
        service = FilterService()
        service.refresh_target_market()

    except Exception as e:
        logger.exception(
            "Error in refresh_target_market: %s",
            str(e),
        )
    logger.info("Finish task: refresh_target_market")
