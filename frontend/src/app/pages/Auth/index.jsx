import { EnvelopeIcon, LockClosedIcon } from "@heroicons/react/24/outline";
import { yupResolver } from "@hookform/resolvers/yup";
import { useForm } from "react-hook-form";

import Logo from "assets/appLogo.svg?react";
import { Button, Card, Input, InputErrorMsg } from "components/ui";
import { useAuthActions } from "hooks/useAuthActions";
import { schema } from "./schema";
import { Page } from "components/shared/Page";

export default function SignIn() {
  const { doLogin, errorMessage, isLoading } = useAuthActions();
  const {
    register,
    handleSubmit,
    formState: { errors },
  } = useForm({
    resolver: yupResolver(schema),
    defaultValues: {
      username: "",
      password: "",
    },
  });

  const onSubmit = (data) => {
    doLogin({
      username: data.username,
      password: data.password,
    });
  };

  return (
    <Page title="Đăng nhập">
      <main className="min-h-100vh grid w-full grow grid-cols-1 place-items-center bg-slate-50 dark:bg-dark-900">
        <div className="w-full max-w-[28rem] p-4 sm:px-6">
          <div className="text-center">
            <Logo className="mx-auto size-16 drop-shadow-sm transition-transform duration-300 hover:scale-105" />
            <div className="mt-6">
              <h2 className="text-2xl font-bold tracking-tight text-gray-900 dark:text-gray-100">
                Chào mừng trở lại
              </h2>
              <p className="mt-1.5 text-sm text-gray-500 dark:text-dark-300">
                Đăng nhập để tiếp tục
              </p>
            </div>
          </div>
          <Card className="mt-8 rounded-2xl border border-gray-100 bg-white p-6 shadow-[0_8px_30px_rgb(0,0,0,0.04)] transition-all duration-300 dark:border-dark-700 dark:bg-dark-800 dark:shadow-none sm:p-8">
            <form onSubmit={handleSubmit(onSubmit)} autoComplete="off">
              <div className="space-y-5">
                <Input
                  label="Tên đăng nhập"
                  placeholder="Nhập tên đăng nhập"
                  prefix={
                    <EnvelopeIcon
                      className="size-5 text-gray-400 transition-colors duration-200"
                      strokeWidth="1.5"
                    />
                  }
                  {...register("username")}
                  error={errors?.username?.message}
                />
                <Input
                  label="Mật khẩu"
                  placeholder="Nhập mật khẩu"
                  type="password"
                  prefix={
                    <LockClosedIcon
                      className="size-5 text-gray-400 transition-colors duration-200"
                      strokeWidth="1.5"
                    />
                  }
                  {...register("password")}
                  error={errors?.password?.message}
                />
              </div>

              <div className="mt-3">
                <InputErrorMsg when={Boolean(errorMessage)}>
                  {errorMessage}
                </InputErrorMsg>
              </div>

              <Button
                type="submit"
                disabled={isLoading}
                className="mt-6 w-full shadow-lg shadow-primary-500/25 transition-all duration-200 hover:-translate-y-0.5 hover:shadow-xl hover:shadow-primary-500/40 active:translate-y-0"
                color="primary"
              >
                {isLoading ? "Đang đăng nhập…" : "Đăng nhập"}
              </Button>
            </form>
          </Card>
        </div>
      </main>
    </Page>
  );
}
