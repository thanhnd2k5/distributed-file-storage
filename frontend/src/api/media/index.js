import { apiAxios } from "../rootApi";
import axios from "axios";

export const getPresignUploadUrlApi = (payload) =>
  apiAxios.post("/api/media/presign-upload", payload).then((res) => res.data?.data ?? res.data);

/**
 * Upload file directly to S3/MinIO using presigned URL
 * @param {string} uploadUrl - The presigned URL
 * @param {File} file - The file to upload
 */
export const uploadToS3Api = (uploadUrl, file, options = {}) => {
  return axios.put(uploadUrl, file, {
    headers: {
      "Content-Type": file.type,
    },
    ...options,
  });
};
