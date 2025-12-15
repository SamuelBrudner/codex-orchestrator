from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta


@dataclass(frozen=True, slots=True)
class NightWindow:
    start: time
    end: time

    def contains(self, local_dt: datetime) -> bool:
        if local_dt.tzinfo is None:
            raise ValueError("NightWindow.contains requires a timezone-aware datetime.")

        t = local_dt.timetz().replace(tzinfo=None)
        if self.start <= self.end:
            return self.start <= t < self.end
        return t >= self.start or t < self.end

    def end_for(self, local_dt: datetime) -> datetime:
        if local_dt.tzinfo is None:
            raise ValueError("NightWindow.end_for requires a timezone-aware datetime.")

        tz = local_dt.tzinfo
        t = local_dt.timetz().replace(tzinfo=None)
        today = local_dt.date()

        if self.start <= self.end:
            if t < self.start:
                end_date = today - timedelta(days=1)
            else:
                end_date = today
        else:
            if t >= self.start:
                end_date = today + timedelta(days=1)
            else:
                end_date = today

        return datetime(
            year=end_date.year,
            month=end_date.month,
            day=end_date.day,
            hour=self.end.hour,
            minute=self.end.minute,
            second=self.end.second,
            microsecond=self.end.microsecond,
            tzinfo=tz,
        )


DEFAULT_NIGHT_WINDOW = NightWindow(start=time(20, 0), end=time(7, 0))

