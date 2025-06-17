from datetime import datetime
import dateutil.tz

def get_rounded_timestamp(frequency_ms):
    """Lấy timestamp hiện tại theo chuẩn ISO8601 và làm tròn theo tần suất."""
    now = datetime.now(dateutil.tz.tzutc())
    microseconds = now.microsecond
    rounding_interval = frequency_ms * 1000 
    rounded_microseconds = (microseconds // rounding_interval) * rounding_interval
    rounded_time = now.replace(microsecond=rounded_microseconds)
    return rounded_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'