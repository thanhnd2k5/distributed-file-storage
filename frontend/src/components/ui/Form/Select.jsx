import PropTypes from "prop-types";
import {
  forwardRef,
  useMemo,
  useRef,
  useState,
  memo,
  useCallback,
} from "react";
import clsx from "clsx";
import { ChevronDownIcon, CheckIcon } from "@heroicons/react/20/solid";

// Local Imports
import { useMergedRef, useId, useListboxPopover } from "hooks";
import { InputErrorMsg } from "./InputErrorMsg";

// ----------------------------------------------------------------------

const Select = memo(
  forwardRef((props, ref) => {
    const {
      label,
      prefix,
      suffix = (
        <ChevronDownIcon className="size-5 text-gray-400 dark:text-dark-300 transition-transform duration-200" />
      ),
      description,
      classNames,
      className,
      error,
      multiple,
      unstyled,
      disabled,
      rootProps,
      labelProps,
      id,
      data = [],
      value,
      defaultValue,
      onChange,
      name,
      placeholder = "Select an option",
      placement = "auto",
      ...rest
    } = props;

    const inputId = useId(id, "select");
    const listboxId = useId(id, "select-listbox");
    const labelId = useId(id, "select-label");
    const buttonRef = useRef(null);
    const dropdownRef = useRef(null);
    const mergedRef = useMergedRef(ref, buttonRef);

    const [isOpen, setIsOpen] = useState(false);
    const closeListbox = useCallback(() => setIsOpen(false), []);

    const { wrapperRef, placementClassName } = useListboxPopover({
      isOpen,
      onClose: closeListbox,
      placement,
      triggerRef: buttonRef,
      dropdownRef,
    });

    const options = useMemo(
      () =>
        data.map((item) => {
          const formatted =
            typeof item !== "object"
              ? { label: String(item), value: item }
              : item;
          return {
            ...formatted,
            disabled: formatted.disabled || false,
          };
        }),
      [data]
    );

    const isControlled = value !== undefined;
    const [internalValue, setInternalValue] = useState(
      defaultValue !== undefined ? defaultValue : multiple ? [] : ""
    );

    const currentValue = isControlled ? value : internalValue;

    const handleChange = useCallback(
      (newValue) => {
        if (!isControlled) {
          setInternalValue(newValue);
        }
        if (onChange) {
          onChange({
            target: {
              name: name || inputId,
              value: newValue,
            },
          });
        }
        if (!multiple) {
          setIsOpen(false);
        }
      },
      [isControlled, multiple, onChange, name, inputId]
    );

    const handleToggle = useCallback(() => {
      if (!disabled) setIsOpen((prev) => !prev);
    }, [disabled]);

    const handleSelect = useCallback(
      (optValue, optDisabled) => {
        if (optDisabled) return;
        if (multiple) {
          const arr = Array.isArray(currentValue) ? currentValue : [];
          const next = arr.includes(optValue)
            ? arr.filter((v) => v !== optValue)
            : [...arr, optValue];
          handleChange(next);
        } else {
          handleChange(optValue);
        }
      },
      [currentValue, multiple, handleChange]
    );

    const selectedDisplay = useMemo(() => {
      if (multiple) {
        if (!Array.isArray(currentValue) || currentValue.length === 0)
          return placeholder;
        const selectedOpts = options.filter((opt) =>
          currentValue.includes(opt.value)
        );
        return selectedOpts.length > 0
          ? selectedOpts.map((o) => o.label).join(", ")
          : placeholder;
      } else {
        const selectedOpt = options.find((opt) => opt.value === currentValue);
        return selectedOpt ? selectedOpt.label : currentValue || placeholder;
      }
    }, [currentValue, multiple, options, placeholder]);

    const isPlaceholder = multiple
      ? !Array.isArray(currentValue) || currentValue.length === 0
      : !currentValue || !options.find((opt) => opt.value === currentValue);

    const affixClass = clsx(
      "pointer-events-none absolute top-0 flex h-full w-9 items-center justify-center transition-colors",
      error
        ? "text-error dark:text-error-light"
        : "text-gray-400 peer-focus:text-primary-600 dark:text-dark-300 dark:peer-focus:text-primary-500"
    );

    return (
      <div className={clsx("input-root", classNames?.root)} {...rootProps}>
        {label && (
          <span
            id={labelId}
            className={clsx("input-label", classNames?.label)}
            {...labelProps}
          >
            <span className={clsx("input-label", classNames?.labelText)}>
              {label}
            </span>
          </span>
        )}

        <div
          ref={wrapperRef}
          className={clsx(
            "input-wrapper relative overflow-visible",
            label && "mt-1.5",
            classNames?.wrapper
          )}
        >
          {/* Trigger Button */}
          <button
            ref={mergedRef}
            id={inputId}
            type="button"
            disabled={disabled}
            aria-haspopup="listbox"
            aria-expanded={isOpen}
            aria-controls={isOpen ? listboxId : undefined}
            aria-labelledby={label ? labelId : undefined}
            onClick={handleToggle}
            className={clsx(
              "relative w-full text-left outline-none",
              multiple ? "form-multiselect-base" : "form-select-base",
              suffix && "ltr:pr-9 rtl:pl-9",
              prefix && "ltr:pl-9 rtl:pr-9",
              !unstyled && [
                multiple ? "form-multiselect" : "form-select",
                error
                  ? "border-error dark:border-error-lighter"
                  : [
                      disabled
                        ? "cursor-not-allowed border-gray-300 bg-gray-150 opacity-60 dark:border-dark-500 dark:bg-dark-600"
                        : "peer cursor-pointer border-gray-300 hover:border-gray-400 focus:border-primary-600 dark:border-dark-450 dark:hover:border-dark-400 dark:focus:border-primary-500",
                    ],
              ],
              className,
              classNames?.select
            )}
            {...rest}
          >
            <span
              className={clsx(
                "block truncate",
                isPlaceholder && "text-gray-400 dark:text-gray-500"
              )}
            >
              {selectedDisplay}
            </span>

            {!unstyled && prefix && (
              <span
                className={clsx(
                  "prefix ltr:left-0 rtl:right-0",
                  affixClass,
                  classNames?.prefix
                )}
              >
                {prefix}
              </span>
            )}
            {!unstyled && suffix && (
              <span
                className={clsx(
                  "suffix ltr:right-0 rtl:left-0",
                  affixClass,
                  isOpen && "rotate-180",
                  classNames?.suffix
                )}
              >
                {suffix}
              </span>
            )}
          </button>

          {/* Dropdown */}
          {isOpen && (
            <div
              ref={dropdownRef}
              id={listboxId}
              role="listbox"
              aria-multiselectable={multiple}
              style={{ animation: "selectDropdownIn 0.15s ease-out" }}
              className={clsx(
                "absolute z-[120] overflow-auto rounded-xl bg-white p-1 text-base shadow-lg ring-1 ring-black/5 focus:outline-none sm:text-sm",
                placementClassName,
                "min-w-full w-max max-h-60",
                "dark:bg-[#1e1e2d] dark:ring-white/10 dark:shadow-2xl",
                classNames?.options
              )}
            >
              {options.length === 0 ? (
                <div className="py-2 px-4 text-sm text-gray-500 dark:text-gray-400 text-center">
                  No options available
                </div>
              ) : (
                options.map((item, index) => {
                  const isSelected = multiple
                    ? Array.isArray(currentValue) &&
                      currentValue.includes(item.value)
                    : currentValue === item.value;

                  return (
                    <div
                      key={item.value ?? index}
                      role="option"
                      aria-selected={isSelected}
                      aria-disabled={item.disabled}
                      onClick={() => handleSelect(item.value, item.disabled)}
                      className={clsx(
                        "relative cursor-pointer select-none py-2.5 pl-10 pr-4 transition-colors rounded-lg mx-1",
                        item.disabled
                          ? "cursor-not-allowed opacity-50"
                          : "hover:bg-primary-50 hover:text-primary-900 dark:hover:bg-primary-500/10 dark:hover:text-primary-400",
                        isSelected
                          ? "bg-primary-50/50 font-semibold dark:bg-primary-500/5 text-primary-700 dark:text-primary-400"
                          : "text-gray-700 dark:text-gray-300 font-normal",
                        classNames?.option
                      )}
                    >
                      <span
                        className={clsx(
                          "block truncate",
                          isSelected ? "font-semibold" : "font-normal"
                        )}
                      >
                        {item.label}
                      </span>
                      {isSelected && (
                        <span className="absolute inset-y-0 left-0 flex items-center pl-3 text-primary-600 dark:text-primary-500">
                          <CheckIcon className="size-5" aria-hidden="true" />
                        </span>
                      )}
                    </div>
                  );
                })
              )}
            </div>
          )}
        </div>

        <InputErrorMsg
          when={error && typeof error !== "boolean"}
          className={classNames?.error}
        >
          {error}
        </InputErrorMsg>
        {description && (
          <span
            className={clsx(
              "input-description mt-1 text-xs text-gray-400 dark:text-dark-300",
              classNames?.description
            )}
          >
            {description}
          </span>
        )}
      </div>
    );
  })
);

Select.displayName = "Select";

Select.propTypes = {
  label: PropTypes.node,
  prefix: PropTypes.node,
  suffix: PropTypes.node,
  description: PropTypes.string,
  className: PropTypes.string,
  classNames: PropTypes.object,
  error: PropTypes.oneOfType([PropTypes.bool, PropTypes.node]),
  unstyled: PropTypes.bool,
  disabled: PropTypes.bool,
  rootProps: PropTypes.object,
  labelProps: PropTypes.object,
  id: PropTypes.string,
  multiple: PropTypes.bool,
  data: PropTypes.array,
  value: PropTypes.any,
  defaultValue: PropTypes.any,
  onChange: PropTypes.func,
  name: PropTypes.string,
  placeholder: PropTypes.string,
  placement: PropTypes.oneOf(["top", "bottom", "auto"]),
};

export { Select };
