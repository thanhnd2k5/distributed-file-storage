import { toast } from "sonner";

export const VALIDATE_EMAIL_REGEX = /^[a-zA-Z0-9][a-zA-Z0-9_.+-]{1,}@[a-z0-9]{1,}(\.[a-z0-9]{1,}){1,2}$/
export const VALIDATE_PASSWORD_REGEX = /^(?=.*?[A-Z])(?=.*?[a-z])(?=.*?[0-9])(?=.*?[^\w\s]).{6,50}$/
export const VALIDATE_PHONE_REGEX_RULE = /^[0-9]{3}[0-9]{3}[0-9]{4}$/

export const handleCheckRoute = (routes, currentRoute) => {
  if (routes && routes.length > 0) {
    return routes.includes(currentRoute);
  }
};

export const isValidEmail = (email) => {
  let result = false
  if (email && typeof email === 'string') {
    const regex = RegExp(VALIDATE_EMAIL_REGEX);
    result = regex.test(email.trim())
  }
  return result
}


export const isValidPassword = (password) => {
  let result = false
  if (password && typeof password === 'string') {
    const regex = RegExp(VALIDATE_PASSWORD_REGEX);
    result = regex.test(password.trim())
  }
  return result
}

export const isValidPhone = (phone) => {
  let result = false

  if (phone && typeof phone === 'string') {
    let trimPhone = phone.trim()

    if (trimPhone) {
      const regexRule = RegExp(VALIDATE_PHONE_REGEX_RULE);

      let ruleMatchs = trimPhone.match(regexRule);

      if (ruleMatchs && ruleMatchs.length > 0) {
        result = (ruleMatchs[0] === trimPhone)
      }
    }
  }
  return result
}



/**
 * Hiển thị thông báo Toast sử dụng thư viện sonner
 * @param {('success'|'error'|'warning'|'info')} type - Loại thông báo
 * @param {string} content - Nội dung thông báo
 */
export const getNotification = (type, content) => {
    switch (type) {
        case "success":
            toast.success(content);
            break;
        case "error":
            toast.error(content);
            break;
        case "warning":
            toast.warning(content);
            break;
        case "info":
            toast.info(content);
            break;
        default:
            toast(content);
    }
};

/**
 * Loại bỏ dấu tiếng Việt khỏi chuỗi
 * @param {string} str 
 * @returns {string}
 */
export const removeAccents = (str) => {
  if (!str || typeof str !== 'string') return '';
  return str
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/đ/g, 'd')
    .replace(/Đ/g, 'D')
    .toLowerCase();
};
