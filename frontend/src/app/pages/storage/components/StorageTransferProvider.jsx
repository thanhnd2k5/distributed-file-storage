import { TransferContext } from "../transferContext";
import { useStorageTransfers } from "../hooks/useStorageTransfers";

export default function StorageTransferProvider({ children }) {
  const transfer = useStorageTransfers();
  return (
    <TransferContext.Provider value={transfer}>
      {children}
    </TransferContext.Provider>
  );
}
