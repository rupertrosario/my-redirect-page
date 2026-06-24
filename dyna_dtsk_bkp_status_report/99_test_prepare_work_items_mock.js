// ==========================================================
// Dynatrace JS Task
// Optional test task: dtsk_prepare_work_items_mock
//
// Purpose:
// - Use only when dtsk_snow_search returns 0 DTSKs
// - Creates one fake work item so loop wiring can be tested
// - Do NOT keep this task in the production workflow path
// ==========================================================

export default async function () {

  const workItems = [
    {
      sysId: "TEST_SYS_ID_ONLY",
      dtsk: "DTSK_TEST_001",
      decomRequest: "DECOM_TEST_001",
      ciName: "server01.company.com",
      aliases: [
        "server01.company.com",
        "server01"
      ],
      ciValid: true,
      assignedTo: "Test User",
      assignmentGroup: "Backup Team",
      assignmentAction: "Assigned",
      createdOn: "2026-06-24 10:00:00",
      state: "2",
      shortDescription: "Mock DTSK for loop wiring test only"
    }
  ];

  const summary = {
    totalDtsks: workItems.length,
    validCiCount: 1,
    invalidCiCount: 0,
    assignedCount: 1,
    unassignedCount: 0,
    mock: true
  };

  console.log("==== MOCK DTSK WORK ITEMS ====");
  console.log(JSON.stringify({ workItems, summary }, null, 2));

  return {
    workItems,
    summary
  };
}
