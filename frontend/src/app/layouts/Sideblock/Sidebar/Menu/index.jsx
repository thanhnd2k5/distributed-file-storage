// Import Dependencies
import { useLocation } from "react-router";
import { useRef, useState } from "react";
import {
  useDidUpdate,
  useIsomorphicEffect,
} from "hooks";
import SimpleBar from "simplebar-react";

// Local Imports
import { navigation } from "app/navigation";
import { Group } from "./Group";
import { Accordion } from "components/ui";
import { isRouteActive } from "utils/isRouteActive";

// ----------------------------------------------------------------------

const isNavActive = (navItem, pathname) => {
  if (navItem.path && isRouteActive(navItem.path, pathname)) {
    return true;
  }
  if (navItem.childs) {
    return navItem.childs.some((child) => isNavActive(child, pathname));
  }
  return false;
};

export function Menu() {
  const { pathname } = useLocation();
  const { ref } = useRef();

  const activeGroup = navigation.find((item) => isNavActive(item, pathname));

  const activeCollapsible = activeGroup?.childs?.find((item) => {
    if (item.childs) {
      return item.childs.some((child) => child.path && isRouteActive(child.path, pathname));
    }
    return item.path && isRouteActive(item.path, pathname);
  });

  const activeKey = activeCollapsible?.path || activeCollapsible?.id || null;

  const [expanded, setExpanded] = useState(activeKey);

  useDidUpdate(() => {
    activeKey !== expanded && setExpanded(activeKey);
  }, [activeKey]);

  useIsomorphicEffect(() => {
    const activeItem = ref?.current.querySelector("[data-menu-active=true]");
    activeItem?.scrollIntoView({ block: "center" });
  }, []);

  return (
    <SimpleBar
      scrollableNodeProps={{ ref }}
      className="min-h-0 flex-1 overflow-x-hidden pb-6"
    >
      <Accordion value={expanded} onChange={setExpanded} className="space-y-1">
        {navigation.map((nav) => (
          <Group key={nav.id} data={nav} />
        ))}
      </Accordion>
    </SimpleBar>
  );
}
