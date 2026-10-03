import { useState } from "react";
import { Page } from "components/shared/Page";

import HeaderSection from "./components/HeaderSection";
import StatCardsGrid from "./components/StatCardsGrid";
import ActionToolbar from "./components/ActionToolbar";
import LinkDataTable from "./components/LinkDataTable";
import SettingsModal from "./components/SettingsModal";

export default function Home() {
  const [isSettingsModalOpen, setIsSettingsModalOpen] = useState(false);

  return (
    <Page title="Tổng quan">
      <div className="transition-content w-full px-4 sm:px-6 lg:px-8 py-6 lg:py-8 max-w-[1600px] mx-auto min-h-screen bg-slate-50/50 dark:bg-gray-900 flex flex-col gap-6 font-sans">

        {/* Header Section */}
        <HeaderSection onOpenSettings={() => setIsSettingsModalOpen(true)} />

        {/* Top KPI Cards */}
        <StatCardsGrid />

        {/* Main Content Area */}
        <div className="flex flex-col gap-4 flex-grow">
          {/* Filters and Actions */}
          <ActionToolbar />

          {/* Data Table */}
          <div className="flex-grow flex flex-col">
            <LinkDataTable />
          </div>
        </div>

        {/* Modals */}
        <SettingsModal
          isOpen={isSettingsModalOpen}
          onClose={() => setIsSettingsModalOpen(false)}
        />
      </div>
    </Page>
  );
}
