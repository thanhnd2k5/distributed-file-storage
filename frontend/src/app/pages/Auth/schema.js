import * as Yup from "yup";

export const schema = Yup.object().shape({
  username: Yup.string().trim().required("Tên đăng nhập là bắt buộc"),
  password: Yup.string().trim().required("Mật khẩu là bắt buộc"),
});
