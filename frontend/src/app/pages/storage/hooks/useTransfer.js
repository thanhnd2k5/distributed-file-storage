import { useContext } from "react";
import { TransferContext } from "../transferContext";

export function useTransfer() {
  const transfer = useContext(TransferContext);
  if (!transfer) throw new Error("Storage transfer provider is missing");
  return transfer;
}
