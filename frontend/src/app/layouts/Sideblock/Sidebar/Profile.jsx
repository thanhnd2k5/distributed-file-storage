// Import Dependencies
import {
  Popover,
  PopoverButton,
  PopoverPanel,
  Transition,
} from "@headlessui/react";
import {
  ArrowLeftStartOnRectangleIcon,
  Cog6ToothIcon,
} from "@heroicons/react/24/outline";
import { TbUser } from "react-icons/tb";
import { Link } from "react-router";
import PropTypes from "prop-types";
import clsx from "clsx";

// Local Imports
import { Avatar, AvatarDot, Button } from "components/ui";
import { useAuthActions } from "hooks/useAuthActions";

// ----------------------------------------------------------------------

const links = [
  {
    id: "1",
    title: "Hồ sơ",
    description: "Thông tin tài khoản",
    to: "/settings/general",
    Icon: TbUser,
    color: "warning",
  },
  {
    id: "3",
    title: "Cài đặt",
    description: "Giao diện ứng dụng",
    to: "/settings/appearance",
    Icon: Cog6ToothIcon,
    color: "success",
  },
];

export function Profile({ isExpanded = false }) {
  const { user, doLogout } = useAuthActions();
  const displayName = user?.full_name || user?.username || "Người dùng";

  return (
    <Popover className="relative w-full">
      <PopoverButton
        className={clsx(
          "group flex w-full cursor-pointer items-center outline-hidden transition-all duration-300",
          isExpanded ? "gap-3 px-5" : "justify-center",
        )}
      >
        <Avatar
          size={isExpanded ? 10 : 12}
          role="button"
          src="/images/200x200.png"
          alt="Profile"
          indicator={
            <AvatarDot color="success" className="ltr:right-0 rtl:left-0" />
          }
          classNames={{
            root: "cursor-pointer shrink-0",
          }}
        />
        {isExpanded && (
          <div className="flex flex-col items-start overflow-hidden text-left animate-in fade-in slide-in-from-left-2 duration-300">
            <span className="dark:text-dark-100 block truncate text-sm font-semibold text-gray-700">
              {displayName}
            </span>
          </div>
        )}
      </PopoverButton>
      <Transition
        enter="duration-200 ease-out"
        enterFrom="translate-y-2 opacity-0"
        enterTo="translate-y-0 opacity-100"
        leave="duration-200 ease-out"
        leaveFrom="translate-y-0 opacity-100"
        leaveTo="translate-y-2 opacity-0"
      >
        <PopoverPanel
          anchor={{ to: isExpanded ? "top" : "right end", gap: 12 }}
          className="border-gray-150 shadow-soft dark:border-dark-600 dark:bg-dark-700 z-70 flex w-64 flex-col rounded-lg border bg-white transition dark:shadow-none"
        >
          {({ close }) => (
            <>
              <div className="dark:bg-dark-800 flex items-center gap-4 rounded-t-lg bg-gray-100 px-4 py-5">
                <Avatar size={14} src="/images/200x200.png" alt="Profile" />
                <div>
                  <Link
                    className="hover:text-primary-600 focus:text-primary-600 dark:text-dark-100 dark:hover:text-primary-400 dark:focus:text-primary-400 text-base font-medium text-gray-700"
                    to="/settings/general"
                  >
                    {displayName}
                  </Link>
                </div>
              </div>
              <div className="flex flex-col pt-2 pb-5">
                {links.map((link) => (
                  <Link
                    key={link.id}
                    to={link.to}
                    onClick={close}
                    className="group dark:hover:bg-dark-600 dark:focus:bg-dark-600 flex items-center gap-3 px-4 py-2 tracking-wide outline-hidden transition-all hover:bg-gray-100 focus:bg-gray-100"
                  >
                    <Avatar
                      size={8}
                      initialColor={link.color}
                      classNames={{ display: "rounded-lg" }}
                    >
                      <link.Icon className="size-4.5" />
                    </Avatar>
                    <div>
                      <h2 className="group-hover:text-primary-600 group-focus:text-primary-600 dark:text-dark-100 dark:group-hover:text-primary-400 dark:group-focus:text-primary-400 font-medium text-gray-800 transition-colors">
                        {link.title}
                      </h2>
                      <div className="dark:text-dark-300 truncate text-xs text-gray-400">
                        {link.description}
                      </div>
                    </div>
                  </Link>
                ))}
                <div className="px-4 pt-4">
                  <Button
                    className="w-full gap-2"
                    onClick={() => {
                      close();
                      doLogout();
                    }}
                  >
                    <ArrowLeftStartOnRectangleIcon className="size-4.5" />
                    <span>Đăng xuất</span>
                  </Button>
                </div>
              </div>
            </>
          )}
        </PopoverPanel>
      </Transition>
    </Popover>
  );
}

Profile.propTypes = {
  isExpanded: PropTypes.bool,
};
