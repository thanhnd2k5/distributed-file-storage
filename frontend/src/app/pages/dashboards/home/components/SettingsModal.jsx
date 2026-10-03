import { Dialog, DialogPanel, DialogTitle, Transition, TransitionChild } from "@headlessui/react";
import { FiX } from "react-icons/fi";
import { Button, Input } from "components/ui";

const SettingsModal = ({ isOpen, onClose }) => {
    return (
        <Transition appear show={isOpen}>
            <Dialog as="div" className="relative z-50" onClose={onClose}>
                <TransitionChild
                    enter="ease-out duration-300"
                    enterFrom="opacity-0"
                    enterTo="opacity-100"
                    leave="ease-in duration-200"
                    leaveFrom="opacity-100"
                    leaveTo="opacity-0"
                >
                    <div className="fixed inset-0 bg-gray-900/50 dark:bg-black/60 backdrop-blur-sm" />
                </TransitionChild>

                <div className="fixed inset-0 overflow-y-auto">
                    <div className="flex min-h-full items-center justify-center p-4 text-center sm:p-0">
                        <TransitionChild
                            enter="ease-out duration-300"
                            enterFrom="opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95"
                            enterTo="opacity-100 translate-y-0 sm:scale-100"
                            leave="ease-in duration-200"
                            leaveFrom="opacity-100 translate-y-0 sm:scale-100"
                            leaveTo="opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95"
                        >
                            <DialogPanel className="relative transform overflow-hidden rounded-2xl bg-white dark:bg-gray-800 text-left shadow-xl transition-all sm:my-8 sm:w-full sm:max-w-xl">

                                {/* Header */}
                                <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 dark:border-gray-700">
                                    <DialogTitle as="h3" className="text-lg font-semibold leading-6 text-gray-900 dark:text-white flex items-center gap-2">
                                        Cài đặt
                                    </DialogTitle>
                                    <button
                                        type="button"
                                        onClick={onClose}
                                        className="rounded-lg p-2 text-gray-400 hover:text-gray-500 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors border border-gray-100 dark:border-gray-700"
                                    >
                                        <FiX className="h-5 w-5" aria-hidden="true" />
                                    </button>
                                </div>

                                {/* Body */}
                                <div className="px-6 py-6 flex flex-col gap-6">
                                    <div>
                                        <Input
                                            label="Telegram Bot Token"
                                            placeholder="Ex: 8743783272:AAFYdqGJcpyfz..."
                                            description="Config trong file .env &rarr; TELEGRAM_BOT_TOKEN"
                                            classNames={{ input: "rounded-xl mt-1 font-mono text-sm" }}
                                        />
                                    </div>
                                    <div>
                                        <div className="flex items-end gap-3 w-full">
                                            <div className="flex-grow">
                                                <Input
                                                    label="Telegram Chat ID"
                                                    placeholder="Ex: 1325181386"
                                                    description="Config trong file .env &rarr; TELEGRAM_CHAT_ID"
                                                    classNames={{ input: "rounded-xl mt-1 font-mono text-sm" }}
                                                />
                                            </div>
                                        </div>
                                    </div>

                                    <Button variant="outlined" color="primary" className="w-full justify-center rounded-xl py-2.5 bg-gray-50 dark:bg-gray-800 hover:bg-gray-100 border-gray-200 dark:border-gray-700">
                                        Test Telegram
                                    </Button>

                                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-6 pt-2">
                                        <Input
                                            type="number"
                                            label="Check interval (phút)"
                                            defaultValue="30"
                                            classNames={{ input: "rounded-xl mt-1" }}
                                        />
                                        <Input
                                            type="number"
                                            label="Concurrency (link song song)"
                                            defaultValue="1"
                                            classNames={{ input: "rounded-xl mt-1" }}
                                        />
                                    </div>
                                </div>

                                {/* Footer */}
                                <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-100 dark:border-gray-700 bg-gray-50/50 dark:bg-gray-800/50">
                                    <Button
                                        variant="outlined"
                                        color="neutral"
                                        onClick={onClose}
                                        className="rounded-xl min-w-[100px]"
                                    >
                                        Đóng
                                    </Button>
                                    <Button
                                        variant="filled"
                                        color="primary"
                                        className="rounded-xl min-w-[100px] shadow-md hover:-translate-y-0.5 transition-transform"
                                        isGlow
                                    >
                                        Lưu
                                    </Button>
                                </div>
                            </DialogPanel>
                        </TransitionChild>
                    </div>
                </div>
            </Dialog>
        </Transition>
    );
};

export default SettingsModal;
