import datetime


def deny(denytime: str):
    """
    系统时间大于 禁止时间 返回 True
    Returns True if the system time is greater than your set time.
    """
    if datetime.datetime.now().strftime("%Y%m%d") >= denytime:
        return False
    return True
