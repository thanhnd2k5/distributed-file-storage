// Import Dependencies
import PropTypes from "prop-types";
import {
  forwardRef,
  useMemo,
  useRef,
  useState,
  memo,
  useCallback,
  useEffect,
} from "react";
import clsx from "clsx";
import { ChevronDownIcon, CheckIcon } from "@heroicons/react/20/solid";
import { XMarkIcon } from "@heroicons/react/24/outline";

// Local Imports
import {
  useId,
  useMergedRef,
  useListboxPopover,
} from "hooks";
import { InputErrorMsg } from "./InputErrorMsg";

// ----------------------------------------------------------------------

const MultiSelect = memo(
  forwardRef((props, ref) => {
    const {
      label,
      placeholder = "Tìm kiếm...",
      description,
      classNames,
      className,
      error,
      disabled = false,
      data = [],
      value = [],
      onChange,
      name,
      id,
      rootProps,
      labelProps,
      placement = "auto",
    } = props;

    const inputId = useId(id, "multiselect");
    const labelId = useId(id, "multiselect-label");
    const listboxId = useId(id, "multiselect-listbox");
    const triggerRef = useRef(null);
    const inputRef = useRef(null);
    const listRef = useRef(null);
    const mergedRef = useMergedRef(ref, inputRef);

    const [isOpen, setIsOpen] = useState(false);
    const [searchText, setSearchText] = useState("");
    const [focusedIndex, setFocusedIndex] = useState(-1);

    const closeListbox = useCallback(() => {
      setIsOpen(false);
      setFocusedIndex(-1);
    }, []);

    const { wrapperRef, placementClassName } = useListboxPopover({
      isOpen,
      onClose: closeListbox,
      placement,
      triggerRef,
      dropdownRef: listRef,
    });

    // Standardize input options format
    const options = useMemo(() =>
      (data || []).map((item) =>
        typeof item !== "object"
          ? { label: String(item), value: item, disabled: false }
          : { disabled: false, ...item }
      ),
      [data]
    );

    // Stable sort partition: Selected items first, then unselected.
    const sortedOptions = useMemo(() => {
      const query = searchText.trim().toLowerCase();

      // Step 1: Filter by search text
      const filtered = query
        ? options.filter((opt) => opt.label.toLowerCase().includes(query))
        : options;

      // Step 2: Partition into selected and unselected
      const selected = filtered.filter((opt) => value.includes(opt.value));
      const unselected = filtered.filter((opt) => !value.includes(opt.value));

      // Step 3: Combine with selected on top
      return [...selected, ...unselected];
    }, [options, value, searchText]);

    // Reset focusedIndex on every search change, not on length change.
    // A same-length but different list would leave focusedIndex pointing at the wrong item.
    useEffect(() => {
      setFocusedIndex(-1);
    }, [searchText]);

    const emitChange = useCallback(
      (newValueArray) => {
        if (onChange) {
          onChange({
            target: {
              name: name || inputId,
              value: newValueArray,
            },
          });
        }
      },
      [onChange, name, inputId]
    );

    const handleSelect = useCallback(
      (optValue, optDisabled) => {
        if (optDisabled || disabled) return;
        if (!value.includes(optValue)) {
          emitChange([...value, optValue]);
        }
        setSearchText("");
        setFocusedIndex(-1);
        inputRef.current?.focus();
      },
      [value, disabled, emitChange]
    );

    const handleDeselect = useCallback(
      (optValue) => {
        if (disabled) return;
        emitChange(value.filter((v) => v !== optValue));
      },
      [value, disabled, emitChange]
    );

    const handleKeyDown = useCallback(
      (e) => {
        if (disabled) return;
        switch (e.key) {
          case "ArrowDown":
            e.preventDefault();
            if (!isOpen) {
              setIsOpen(true);
            } else {
              setFocusedIndex((prev) =>
                prev < sortedOptions.length - 1 ? prev + 1 : prev
              );
            }
            break;

          case "ArrowUp":
            e.preventDefault();
            if (isOpen) {
              setFocusedIndex((prev) =>
                prev > 0 ? prev - 1 : sortedOptions.length - 1
              );
            }
            break;

          case "Enter":
            e.preventDefault();
            if (isOpen) {
              if (focusedIndex >= 0 && focusedIndex < sortedOptions.length) {
                const opt = sortedOptions[focusedIndex];
                if (value.includes(opt.value)) {
                  handleDeselect(opt.value);
                } else {
                  handleSelect(opt.value, opt.disabled);
                }
              }
            } else {
              setIsOpen(true);
            }
            break;

          case "Backspace":
            if (searchText === "" && value.length > 0) {
              handleDeselect(value[value.length - 1]);
            }
            break;

          case "Escape":
            closeListbox();
            break;

          default:
            break;
        }
      },
      [
        disabled,
        isOpen,
        sortedOptions,
        focusedIndex,
        value,
        searchText,
        handleSelect,
        handleDeselect,
        closeListbox,
      ]
    );

    // Auto-scroll focused item into view
    useEffect(() => {
      if (focusedIndex < 0 || !listRef.current) return;
      const items = listRef.current.querySelectorAll("[data-option-item]");
      items[focusedIndex]?.scrollIntoView({ block: "nearest" });
    }, [focusedIndex]);

    const handleContainerClick = () => {
      if (!disabled) {
        setIsOpen(true);
        inputRef.current?.focus();
      }
    };

    const affixClass = clsx(
      "pointer-events-none absolute right-2.5 top-1/2 -translate-y-1/2 flex items-center justify-center transition-colors",
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
          {/* Trigger Input Area */}
          <div
            ref={triggerRef}
            onClick={handleContainerClick}
            className={clsx(
              "relative w-full text-left outline-none transition-all duration-200",
              "flex flex-wrap items-center gap-1 min-h-0 rounded-lg sm:text-sm py-2 pl-3 pr-9 shadow-sm",
              error
                ? "border border-error focus-within:ring-1 focus-within:ring-error dark:border-error-lighter"
                : "border border-gray-300 hover:border-gray-400 focus-within:border-primary-500 focus-within:ring-1 focus-within:ring-primary-500 dark:border-dark-450 dark:hover:border-dark-400 dark:focus-within:border-primary-500",
              disabled
                ? "cursor-not-allowed bg-gray-150 text-gray-500 opacity-60 dark:bg-dark-600 dark:text-dark-400"
                : "bg-white text-gray-900 cursor-pointer dark:bg-[#1e1e2d] dark:text-dark-50",
              className,
              classNames?.select
            )}
          >
            {/* Selected items badges */}
            {value.map((val) => {
              const opt = options.find((o) => o.value === val);
              return (
                <span
                  key={val}
                  className="inline-flex items-center gap-1 bg-primary-50 text-primary-700 dark:bg-primary-500/10 dark:text-primary-400 pl-2.5 pr-1.5 py-0.5 rounded-lg text-xs font-medium border border-primary-100 dark:border-primary-500/20 max-w-[200px]"
                >
                  <span className="truncate">{opt?.label ?? val}</span>
                  {!disabled && (
                    <button
                      type="button"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleDeselect(val);
                      }}
                      className="hover:bg-primary-100 dark:hover:bg-primary-500/20 rounded p-0.5 text-primary-600 dark:text-primary-400 focus:outline-none transition-colors"
                    >
                      <XMarkIcon className="size-3" />
                    </button>
                  )}
                </span>
              );
            })}

            {/* Interactive Search Input */}
            <input
              ref={mergedRef}
              id={inputId}
              type="text"
              role="combobox"
              aria-expanded={isOpen}
              aria-controls={isOpen ? listboxId : undefined}
              aria-labelledby={label ? labelId : undefined}
              value={searchText}
              onChange={(e) => {
                setSearchText(e.target.value);
                if (!disabled) setIsOpen(true);
              }}
              onKeyDown={handleKeyDown}
              placeholder={value.length === 0 ? placeholder : ""}
              disabled={disabled}
              className="flex-1 min-w-[60px] bg-transparent border-0 p-0 text-sm focus:ring-0 outline-none text-gray-900 dark:text-dark-50 placeholder-gray-400 dark:placeholder-gray-500"
            />

            {/* Suffix chevron */}
            <span className={affixClass}>
              <ChevronDownIcon
                className={clsx(
                  "size-5 text-gray-400 dark:text-dark-300 transition-transform duration-200",
                  isOpen && "rotate-180"
                )}
              />
            </span>
          </div>

          {/* Dropdown Options */}
          {isOpen && (
            <div
              ref={listRef}
              id={listboxId}
              role="listbox"
              aria-multiselectable="true"
              style={{ animation: "selectDropdownIn 0.15s ease-out" }}
              className={clsx(
                "absolute z-[120] overflow-auto rounded-xl bg-white p-1 text-base shadow-lg ring-1 ring-black/5 focus:outline-none sm:text-sm",
                placementClassName,
                "min-w-full w-max max-h-60",
                "dark:bg-[#1e1e2d] dark:ring-white/10 dark:shadow-2xl",
                classNames?.options
              )}
            >
              {sortedOptions.length === 0 ? (
                <div className="py-3 px-4 text-sm text-gray-500 dark:text-gray-400 text-center">
                  Không tìm thấy kết quả
                </div>
              ) : (
                sortedOptions.map((opt, index) => {
                  const isSelected = value.includes(opt.value);
                  const isFocused = index === focusedIndex;

                  return (
                    <div
                      key={opt.value ?? index}
                      data-option-item
                      role="option"
                      aria-selected={isSelected}
                      aria-disabled={opt.disabled}
                      onClick={() => {
                        if (isSelected) {
                          handleDeselect(opt.value);
                        } else {
                          handleSelect(opt.value, opt.disabled);
                        }
                      }}
                      className={clsx(
                        "relative cursor-pointer select-none py-2.5 pl-10 pr-4 transition-colors rounded-lg mx-1",
                        opt.disabled
                          ? "cursor-not-allowed opacity-50"
                          : [
                              isFocused
                                ? "bg-primary-50 text-primary-900 dark:bg-primary-500/10 dark:text-primary-400"
                                : "hover:bg-primary-50 hover:text-primary-900 dark:hover:bg-primary-500/10 dark:hover:text-primary-400",
                            ],
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
                        {opt.label}
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

MultiSelect.displayName = "MultiSelect";

MultiSelect.propTypes = {
  label: PropTypes.node,
  placeholder: PropTypes.string,
  description: PropTypes.string,
  className: PropTypes.string,
  classNames: PropTypes.object,
  error: PropTypes.oneOfType([PropTypes.bool, PropTypes.node]),
  disabled: PropTypes.bool,
  data: PropTypes.array,
  value: PropTypes.array,
  onChange: PropTypes.func,
  name: PropTypes.string,
  id: PropTypes.string,
  rootProps: PropTypes.object,
  labelProps: PropTypes.object,
  placement: PropTypes.oneOf(["top", "bottom", "auto"]),
};

export { MultiSelect };
