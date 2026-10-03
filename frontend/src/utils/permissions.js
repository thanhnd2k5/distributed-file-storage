export function hasPermission(user, code) {
  return user?.permissions?.includes(code) ?? false;
}

export function hasAnyPermission(user, codes) {
  return codes.some((code) => hasPermission(user, code));
}
