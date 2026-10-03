export const locales = {
  vi: {
    label: "Tiếng Việt",
    dayjs: () => import("dayjs/locale/vi"),
    i18n: () => import("./locales/vi/translations.json"),
    flag: "vietnam",
  },
  en: {
    label: "English",
    dayjs: () => import("dayjs/locale/en"),
    i18n: () => import("./locales/en/translations.json"),
    flag: "united-kingdom",
  },
};
