import logging

log_file = "my_app.log"
log_level = logging.DEBUG

logging.basicConfig(
    filename=log_file,
    level=log_level,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.debug("디버그 메시지입니다.")
logging.info("정보 메시지입니다.")
logging.warning("경고 메시지입니다.")
logging.error("에러 메시지입니다.")
logging.critical("심각한 오류 메시지입니다.")