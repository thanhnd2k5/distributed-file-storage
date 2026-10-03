// Import Dependencies
import {
  Popover,
  PopoverButton,
  PopoverPanel,
  Transition,
} from "@headlessui/react";
import {
  CalendarIcon,
  ChevronDownIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  XMarkIcon,
} from "@heroicons/react/24/outline";
import clsx from "clsx";
import dayjs from "dayjs";
import { enUS, vi } from "date-fns/locale";
import PropTypes from "prop-types";
import { Fragment, useEffect, useState } from "react";
import { DayPicker, getDefaultClassNames } from "react-day-picker";
import "react-day-picker/style.css";

// Local Imports
import { useLocaleContext } from "app/contexts/locale/context";
import { Button, Input } from "components/ui";

// ----------------------------------------------------------------------

const DATE_LOCALES = {
  vi,
  en: enUS,
};

const EMPTY_RANGE = { from: "", to: "" };

const defaults = getDefaultClassNames();

/**
 * Header uses CSS grid so prev/next arrows sit beside month+year
 * without DayPicker "around" absolute layout splitting the dropdowns.
 */
const dayPickerClassNames = {
  root: clsx(
    defaults.root,
    "w-fit max-w-full overflow-hidden",
    "[--rdp-accent-color:var(--color-primary-600)]",
    "[--rdp-accent-background-color:var(--color-primary-50)]",
    "[--rdp-today-color:var(--color-primary-600)]",
    "[--rdp-day-height:2rem]",
    "[--rdp-day-width:2.25rem]",
    "[--rdp-day_button-height:2rem]",
    "[--rdp-day_button-width:2.25rem]",
    "[--rdp-day_button-border-radius:0.5rem]",
    "[--rdp-selected-border:none]",
    "[--rdp-nav-height:2.25rem]",
    "[--rdp-nav_button-height:1.75rem]",
    "[--rdp-nav_button-width:1.75rem]",
    "[--rdp-dropdown-gap:0.25rem]",
    "[--rdp-weekday-padding:0.25rem_0]",
    "[--rdp-weekday-opacity:1]",
    "dark:[--rdp-accent-color:var(--color-primary-500)]",
    "dark:[--rdp-accent-background-color:rgb(99_102_241_/_0.15)]",
    "dark:[--rdp-today-color:var(--color-primary-400)]",
  ),
  months: clsx(defaults.months, "relative w-fit max-w-full!"),
  month: clsx(
    defaults.month,
    "relative grid w-fit grid-cols-[1.75rem_auto_1.75rem] grid-rows-[2.25rem_auto] items-center",
  ),
  month_caption: clsx(
    defaults.month_caption,
    "col-start-2 row-start-1 mx-0! flex h-9 items-center justify-center justify-self-center",
  ),
  caption_label: clsx(
    defaults.caption_label,
    "relative z-1 inline-flex items-center whitespace-nowrap text-[13px] font-semibold tracking-tight text-gray-800 dark:text-dark-50",
  ),
  dropdowns: clsx(
    defaults.dropdowns,
    "relative z-10 inline-flex! w-max! items-center justify-center gap-1.5",
  ),
  dropdown_root: clsx(
    defaults.dropdown_root,
    "relative inline-flex! w-auto! shrink-0 cursor-pointer items-center rounded-lg px-1.5 py-1 transition hover:bg-gray-100 dark:hover:bg-dark-600",
  ),
  months_dropdown: clsx(defaults.months_dropdown, "w-auto!"),
  years_dropdown: clsx(defaults.years_dropdown, "w-auto!"),
  dropdown: clsx(
    defaults.dropdown,
    "absolute inset-0 z-2 m-0 w-full cursor-pointer appearance-none opacity-0!",
  ),
  button_previous: clsx(
    defaults.button_previous,
    "static! relative! top-auto! left-auto! col-start-1 row-start-1 inline-flex size-7 items-center justify-center justify-self-center rounded-full text-gray-500 transition hover:bg-gray-100 hover:text-primary-600 dark:text-dark-200 dark:hover:bg-dark-600 dark:hover:text-primary-400",
  ),
  button_next: clsx(
    defaults.button_next,
    "static! relative! top-auto! right-auto! col-start-3 row-start-1 inline-flex size-7 items-center justify-center justify-self-center rounded-full text-gray-500 transition hover:bg-gray-100 hover:text-primary-600 dark:text-dark-200 dark:hover:bg-dark-600 dark:hover:text-primary-400",
  ),
  chevron: clsx(defaults.chevron, "fill-none!"),
  month_grid: clsx(
    defaults.month_grid,
    "col-span-3 row-start-2 w-fit table-fixed border-collapse",
  ),
  weekdays: defaults.weekdays,
  weekday: clsx(
    defaults.weekday,
    "box-border w-9! max-w-9 p-0! text-center text-[11px] font-medium text-primary-600 dark:text-primary-400",
  ),
  weeks: defaults.weeks,
  week: defaults.week,
  day: clsx(
    defaults.day,
    "box-border h-8! w-9! max-w-9 p-0! text-center text-xs-plus",
  ),
  day_button: clsx(
    defaults.day_button,
    "box-border inline-flex h-8! w-9! items-center justify-center rounded-lg text-xs-plus font-medium text-gray-600 transition hover:bg-gray-150 focus-visible:outline-hidden dark:text-dark-100 dark:hover:bg-dark-500",
  ),
  selected: clsx(
    defaults.selected,
    "text-xs-plus! font-semibold [&>button]:border-transparent! [&>button]:bg-primary-600! [&>button]:text-white! [&>button]:hover:bg-primary-600! [&>button]:hover:text-white! dark:[&>button]:bg-primary-500! dark:[&>button]:hover:bg-primary-500!",
  ),
  range_start: clsx(
    defaults.range_start,
    "rounded-l-lg [&>button]:rounded-lg",
  ),
  range_middle: clsx(
    defaults.range_middle,
    "[&>button]:rounded-none [&>button]:bg-transparent! [&>button]:text-primary-700! dark:[&>button]:text-primary-300!",
  ),
  range_end: clsx(defaults.range_end, "rounded-r-lg [&>button]:rounded-lg"),
  today: clsx(
    defaults.today,
    "[&:not(.rdp-selected):not(.rdp-range_start):not(.rdp-range_end)>button]:border [&:not(.rdp-selected):not(.rdp-range_start):not(.rdp-range_end)>button]:border-gray-200 [&:not(.rdp-selected):not(.rdp-range_start):not(.rdp-range_end)>button]:font-semibold dark:[&:not(.rdp-selected):not(.rdp-range_start):not(.rdp-range_end)>button]:border-dark-450",
  ),
  outside: clsx(
    defaults.outside,
    "opacity-100! [&>button]:text-gray-400 [&>button]:hover:bg-transparent dark:[&>button]:text-dark-300",
  ),
  disabled: clsx(
    defaults.disabled,
    "[&>button]:cursor-not-allowed [&>button]:opacity-40 [&>button]:hover:bg-transparent",
  ),
};

const parseValue = (value) => {
  if (!value) return undefined;
  const parsed = dayjs(value);
  return parsed.isValid() ? parsed.startOf("day").toDate() : undefined;
};

const formatValue = (date) => (date ? dayjs(date).format("YYYY-MM-DD") : "");

const formatDisplay = (value) =>
  value && dayjs(value).isValid() ? dayjs(value).format("DD/MM/YYYY") : "";

const normalizeRange = (value) => {
  if (!value || typeof value !== "object") return EMPTY_RANGE;
  return {
    from: value.from || "",
    to: value.to || "",
  };
};

const toDayPickerRange = (value) => {
  const range = normalizeRange(value);
  const from = parseValue(range.from);
  if (!from) return undefined;
  return {
    from,
    to: parseValue(range.to),
  };
};

const getDisplayValue = (mode, value) => {
  if (mode === "range") {
    const range = normalizeRange(value);
    if (range.from && range.to) {
      return `${formatDisplay(range.from)} – ${formatDisplay(range.to)}`;
    }
    if (range.from) return `${formatDisplay(range.from)} – …`;
    return "";
  }
  return formatDisplay(value);
};

const hasValue = (mode, value) => {
  if (mode === "range") {
    const range = normalizeRange(value);
    return Boolean(range.from || range.to);
  }
  return Boolean(value);
};

function Chevron({ orientation }) {
  const iconClass = "size-4 fill-none! stroke-current stroke-[1.75]";
  if (orientation === "left") {
    return <ChevronLeftIcon className={iconClass} />;
  }
  if (orientation === "down") {
    return (
      <ChevronDownIcon
        className={clsx(iconClass, "ml-0.5 size-3.5 opacity-50")}
      />
    );
  }
  return <ChevronRightIcon className={iconClass} />;
}

Chevron.propTypes = {
  orientation: PropTypes.string,
};

/**
 * DatePicker — single day or date range.
 *
 * mode="single" (default)
 *   value: "YYYY-MM-DD" | ""
 *   onChange: (value: string) => void
 *
 * mode="range"
 *   value: { from: string, to: string }
 *   onChange: (value: { from: string, to: string }) => void
 *
 * min / max: "YYYY-MM-DD" bounds (disabled days), not DayPicker range length.
 */
function DatePicker({
  mode = "single",
  value,
  onChange,
  label,
  error,
  placeholder,
  min,
  max,
  disabled = false,
  className,
  classNames,
}) {
  const isRange = mode === "range";
  const resolvedPlaceholder =
    placeholder || (isRange ? "Chọn khoảng ngày" : "Chọn ngày");
  const resolvedValue = isRange
    ? normalizeRange(value)
    : typeof value === "string"
      ? value
      : "";

  const { locale } = useLocaleContext();
  const selectedSingle = isRange ? undefined : parseValue(resolvedValue);
  const selectedRange = isRange ? toDayPickerRange(resolvedValue) : undefined;
  const minDate = parseValue(min);
  const maxDate = parseValue(max);

  const anchorDate =
    (isRange ? selectedRange?.from : selectedSingle) || minDate || new Date();
  const [month, setMonth] = useState(anchorDate);

  useEffect(() => {
    const next = isRange ? selectedRange?.from : selectedSingle;
    if (next) setMonth(next);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- sync from serialized value only
  }, [isRange ? resolvedValue.from : resolvedValue]);

  const disabledMatchers = [];
  if (minDate) disabledMatchers.push({ before: minDate });
  if (maxDate) disabledMatchers.push({ after: maxDate });

  const handleClear = (event) => {
    event.preventDefault();
    event.stopPropagation();
    onChange?.(isRange ? { ...EMPTY_RANGE } : "");
  };

  const handleSelectSingle = (date, close) => {
    if (!date) return;
    onChange?.(formatValue(date));
    close();
  };

  const handleSelectRange = (range, close) => {
    if (!range?.from) {
      onChange?.({ ...EMPTY_RANGE });
      return;
    }
    const next = {
      from: formatValue(range.from),
      to: range.to ? formatValue(range.to) : "",
    };
    onChange?.(next);
    if (range.from && range.to) close();
  };

  return (
    <Popover className={clsx("relative", classNames?.root)}>
      {({ close }) => (
        <>
          <PopoverButton
            as="div"
            disabled={disabled}
            className={clsx(
              "w-full outline-hidden",
              disabled ? "cursor-not-allowed" : "cursor-pointer",
            )}
          >
            <Input
              readOnly
              disabled={disabled}
              label={label}
              error={error}
              placeholder={resolvedPlaceholder}
              value={getDisplayValue(mode, resolvedValue)}
              className={clsx("cursor-pointer", className)}
              classNames={classNames}
              prefix={<CalendarIcon className="size-5 stroke-[1.5]" />}
              suffix={
                hasValue(mode, resolvedValue) && !disabled ? (
                  <Button
                    type="button"
                    isIcon
                    variant="flat"
                    className="pointer-events-auto size-6 rounded-full"
                    onClick={handleClear}
                    aria-label="Xóa ngày"
                  >
                    <XMarkIcon className="size-4" />
                  </Button>
                ) : null
              }
            />
          </PopoverButton>

          <Transition
            as={Fragment}
            enter="transition ease-out duration-150"
            enterFrom="opacity-0 translate-y-1"
            enterTo="opacity-100 translate-y-0"
            leave="transition ease-in duration-100"
            leaveFrom="opacity-100 translate-y-0"
            leaveTo="opacity-0 translate-y-1"
          >
            <PopoverPanel
              anchor={{ to: "bottom start", gap: 8 }}
              className="z-110 w-fit max-w-[18rem] overflow-hidden rounded-xl border border-gray-150 bg-white p-3 shadow-lg outline-hidden dark:border-dark-500 dark:bg-dark-700 dark:shadow-none"
            >
              {isRange ? (
                <DayPicker
                  mode="range"
                  required={false}
                  resetOnSelect
                  showOutsideDays
                  fixedWeeks
                  captionLayout="dropdown"
                  navLayout="around"
                  month={month}
                  onMonthChange={setMonth}
                  startMonth={
                    minDate ||
                    dayjs().subtract(30, "year").startOf("year").toDate()
                  }
                  endMonth={
                    maxDate || dayjs().add(10, "year").endOf("year").toDate()
                  }
                  locale={DATE_LOCALES[locale] || vi}
                  selected={selectedRange}
                  disabled={
                    disabledMatchers.length > 0 ? disabledMatchers : undefined
                  }
                  onSelect={(range) => handleSelectRange(range, close)}
                  components={{ Chevron }}
                  classNames={dayPickerClassNames}
                />
              ) : (
                <DayPicker
                  mode="single"
                  required
                  showOutsideDays
                  fixedWeeks
                  captionLayout="dropdown"
                  navLayout="around"
                  month={month}
                  onMonthChange={setMonth}
                  startMonth={
                    minDate ||
                    dayjs().subtract(30, "year").startOf("year").toDate()
                  }
                  endMonth={
                    maxDate || dayjs().add(10, "year").endOf("year").toDate()
                  }
                  locale={DATE_LOCALES[locale] || vi}
                  selected={selectedSingle}
                  disabled={
                    disabledMatchers.length > 0 ? disabledMatchers : undefined
                  }
                  onSelect={(date) => handleSelectSingle(date, close)}
                  components={{ Chevron }}
                  classNames={dayPickerClassNames}
                />
              )}
            </PopoverPanel>
          </Transition>
        </>
      )}
    </Popover>
  );
}

DatePicker.displayName = "DatePicker";

DatePicker.propTypes = {
  mode: PropTypes.oneOf(["single", "range"]),
  value: PropTypes.oneOfType([
    PropTypes.string,
    PropTypes.shape({
      from: PropTypes.string,
      to: PropTypes.string,
    }),
  ]),
  onChange: PropTypes.func,
  label: PropTypes.node,
  error: PropTypes.oneOfType([PropTypes.bool, PropTypes.node]),
  placeholder: PropTypes.string,
  min: PropTypes.string,
  max: PropTypes.string,
  disabled: PropTypes.bool,
  className: PropTypes.string,
  classNames: PropTypes.object,
};

export { DatePicker };
