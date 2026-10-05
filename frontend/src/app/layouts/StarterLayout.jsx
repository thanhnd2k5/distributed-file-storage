import { Outlet } from "react-router";

import { AuthBootstrap } from "app/AuthBootstrap";
import { SplashScreen } from "components/template/SplashScreen";
import { useAuthActions } from "hooks/useAuthActions";

function InitializedOutlet() {
  const { isInitialized } = useAuthActions();
  return isInitialized ? <Outlet /> : <SplashScreen />;
}

export default function StarterLayout() {
  return (
    <AuthBootstrap>
      <InitializedOutlet />
    </AuthBootstrap>
  );
}
