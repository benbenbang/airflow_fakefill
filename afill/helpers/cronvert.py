# standard library
import re

# pypi/conda library
from loguru import logger

DAY_NAMES = list(zip(("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"), list(range(7))))
MINUTES = (0, 59)
HOURS = (0, 23)
DAYS_OF_MONTH = (1, 31)
MONTHS = (1, 12)
DAYS_OF_WEEK = (0, 6)
L_FIELDS = (DAYS_OF_WEEK, DAYS_OF_MONTH)
FIELD_RANGES = (MINUTES, HOURS, DAYS_OF_MONTH, MONTHS, DAYS_OF_WEEK)
MONTH_NAMES = list(
    zip(("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), list(range(1, 13)))
)
DEFAULT_EPOCH = (1970, 1, 1, 0, 0, 0)
SUBSTITUTIONS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}
VALIDATE_POUND = re.compile("^[0-6]#[1-5]")
VALIDATE_L_IN_DOW = re.compile("^[0-6]L$")
VALIDATE_W = re.compile("^[0-3]?[0-9]W$")


def cron_counts(cronexpr: str, num: int) -> int:
    try:
        return __CronToMonthly__(cronexpr)(num)
    except Exception:
        logger.warning(f"Cannot parse cron expression: {cronexpr}. Using default value: 30")
        return 30


class __CronToMonthly__:
    def __init__(self, line: str, epoch=DEFAULT_EPOCH, epoch_utc_offset: int = 0):
        """
        Instantiates a CronExpression object with an optionally defined epoch.
        If the epoch is defined, the UTC offset can be specified one of two
        ways: as the sixth element in 'epoch' or supplied in epoch_utc_offset.
        The epoch should be defined down to the minute sorted by
        descending significance.
        """
        for key, value in list(SUBSTITUTIONS.items()):
            if line.startswith(key):
                line = line.replace(key, value)
                break

        fields = line.split(None, 5)
        if len(fields) == 5:
            fields.append("")

        minutes, hours, dom, months, dow, self.comment = fields

        dow = dow.replace("7", "0").replace("?", "*")
        dom = dom.replace("?", "*")

        for monthstr, monthnum in MONTH_NAMES:
            months = months.upper().replace(monthstr, str(monthnum))

        for dowstr, downum in DAY_NAMES:
            dow = dow.upper().replace(dowstr, str(downum))

        self.string_tab = [minutes, hours, dom, months, dow]
        self.compute_numtab()
        if len(epoch) == 5:
            y, mo, d, h, m = epoch
            self.epoch = (y, mo, d, h, m, epoch_utc_offset)
        else:
            self.epoch = epoch

    def __call__(self, num: int = 3) -> int:
        """ This method is just to calculate an approximate upper limit.
        Crontab like `* 8 1 * Sun` will be round up to "every minute past hour 8 on first day of month"
        i.e. Weekday will be dropped if get month day
        """
        minute, hour, day, _, _ = self.numerical_tab

        # Get execution times
        count_min = len(minute)
        count_hour = len(hour)
        count_day = len(day)

        base_unit = 1

        base_unit *= count_min if count_min else base_unit
        base_unit *= count_hour if count_hour else base_unit
        if base_unit >= 1024:
            return base_unit
        else:
            base_unit *= count_day if count_day else base_unit
            return base_unit * num

    def __repr__(self):
        base = self.__class__.__name__ + "(%s)"
        cron_line = self.string_tab + [str(self.comment)]
        if not self.comment:
            cron_line.pop()
        arguments = '"' + " ".join(cron_line) + '"'
        if self.epoch != DEFAULT_EPOCH:
            return base % (arguments + ", epoch=" + repr(self.epoch))
        else:
            return base % arguments

    def __str__(self):
        return repr(self)

    def compute_numtab(self):
        """
        Recomputes the sets for the static ranges of the trigger time.

        This method should only be called by the user if the string_tab
        member is modified.
        """
        self.numerical_tab = []

        for field_str, span in zip(self.string_tab, FIELD_RANGES):
            split_field_str = field_str.split(",")
            if len(split_field_str) > 1 and "*" in split_field_str:
                raise ValueError('"*" must be alone in a field.')

            unified = set()
            for cron_atom in split_field_str:
                # parse_atom only handles static cases
                if not (is_special_atom(cron_atom, span)):
                    unified.update(parse_atom(cron_atom, span))

            self.numerical_tab.append(unified)

        if self.string_tab[2] == "*" and self.string_tab[4] != "*":
            self.numerical_tab[2] = set()
        elif self.string_tab[4] == "*" and self.string_tab[2] != "*":
            self.numerical_tab[4] = set()


def is_special_atom(cron_atom, span):
    """
    Returns a boolean indicating whether or not the string can be parsed by
    parse_atom to produce a static set. In the process of examining the
    string, the syntax of any special character uses is also checked.
    """
    for special_char in ("%", "#", "L", "W"):
        if special_char not in cron_atom:
            continue

        if special_char == "#":
            if span != DAYS_OF_WEEK:
                raise ValueError('"#" invalid where used.')
            elif not VALIDATE_POUND.match(cron_atom):
                raise ValueError('"#" syntax incorrect.')
        elif special_char == "W":
            if span != DAYS_OF_MONTH:
                raise ValueError('"W" syntax incorrect.')
            elif not (VALIDATE_W.match(cron_atom) and int(cron_atom[:-1]) > 0):
                raise ValueError('Invalid use of "W".')
        elif special_char == "L":
            if span not in L_FIELDS:
                raise ValueError('"L" invalid where used.')
            elif span == DAYS_OF_MONTH:
                if cron_atom != "L":
                    raise ValueError('"L" must be alone in days of month.')
            elif span == DAYS_OF_WEEK:
                if not VALIDATE_L_IN_DOW.match(cron_atom):
                    raise ValueError('"L" syntax incorrect.')
        elif special_char == "%":
            if not (cron_atom[1:].isdigit() and int(cron_atom[1:]) > 1):
                raise ValueError('"%" syntax incorrect.')
        return True
    else:
        return False


def parse_atom(parse, minmax):
    """
    Returns a set containing valid values for a given cron-style range of
    numbers. The 'minmax' arguments is a two element iterable containing the
    inclusive upper and lower limits of the expression.

    Examples:
    >>> parse_atom("1-5",(0,6))
    set([1, 2, 3, 4, 5])

    >>> parse_atom("*/6",(0,23))
    set([0, 6, 12, 18])

    >>> parse_atom("18-6/4",(0,23))
    set([18, 22, 0, 4])

    >>> parse_atom("*/9",(0,23))
    set([0, 9, 18])
    """
    parse = parse.strip()
    increment = 1
    if parse == "*":
        return set(range(minmax[0], minmax[1] + 1))
    elif parse.isdigit():
        # A single number still needs to be returned as a set
        value = int(parse)
        if value >= minmax[0] and value <= minmax[1]:
            return set((value,))
        else:
            raise ValueError('"%s" is not within valid range.' % parse)
    elif "-" in parse or "/" in parse:
        divide = parse.split("/")
        subrange = divide[0]
        if len(divide) == 2:
            # Example: 1-3/5 or */7 increment should be 5 and 7 respectively
            increment = int(divide[1])

        if "-" in subrange:
            # Example: a-b
            prefix, suffix = [int(n) for n in subrange.split("-")]
            if prefix < minmax[0] or suffix > minmax[1]:
                raise ValueError('"%s" is not within valid range.' % parse)
        elif subrange.isdigit():
            # Handle offset increments e.g. 5/15 to run at :05, :20, :35, and :50
            return set(range(int(subrange), minmax[1] + 1, increment))
        elif subrange == "*":
            # Include all values with the given range
            prefix, suffix = minmax
        else:
            raise ValueError('Unrecognized symbol "%s"' % subrange)

        if prefix < suffix:
            # Example: 7-10
            return set(range(prefix, suffix + 1, increment))
        else:
            # Example: 12-4/2; (12, 12 + n, ..., 12 + m*n) U (n_0, ..., 4)
            noskips = list(range(prefix, minmax[1] + 1))
            noskips += list(range(minmax[0], suffix + 1))
            return set(noskips[::increment])
    else:
        raise ValueError('Atom "%s" not in a recognized format.' % parse)
