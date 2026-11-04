import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import UploadV2Page from "../src/app/pages/storageV2";
import "../src/styles/index.css";

createRoot(document.querySelector("#root")).render(
  <MemoryRouter>
    <UploadV2Page />
  </MemoryRouter>,
);
document.querySelector("#select-fixture").addEventListener("click", () => {
  const bytes = new Uint8Array(65537);
  for (let i = 0; i < bytes.length; i++) bytes[i] = (i * 31 + 93) % 256;
  const transfer = new DataTransfer();
  transfer.items.add(
    new File([bytes], "ui-source.bin", { type: "application/octet-stream" }),
  );
  const input = document.querySelector("#v2-source");
  input.files = transfer.files;
  input.dispatchEvent(new Event("change", { bubbles: true }));
});
