import PropTypes from "prop-types";
import { ConfirmModal } from "./ConfirmModal";

const MESSAGES = {
  pending: {
    title: "Thoát mà chưa lưu?",
    description:
      "Bạn có thay đổi chưa được lưu. Nếu thoát bây giờ, các thay đổi sẽ bị mất.",
    actionText: "Thoát",
  },
};

export function FormExitConfirmModal({ show, onConfirm, onCancel }) {
  return (
    <ConfirmModal
      show={show}
      state="pending"
      messages={MESSAGES}
      onClose={onCancel}
      onOk={onConfirm}
    />
  );
}

FormExitConfirmModal.propTypes = {
  show: PropTypes.bool,
  onConfirm: PropTypes.func,
  onCancel: PropTypes.func,
};
