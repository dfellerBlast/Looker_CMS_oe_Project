connection: "cms_medicare_ga360"

include: "/views/*.view.lkml"

week_start_day: sunday

explore: pre_oe_weekly_oe_to_date {}
explore: pre_oe_weekly_planfindermetrics {}
explore: pre_oe_weekly_medicareaccounts {}
explore: pre_oe_weekly_medicaresitewide {}
explore: pre_oe_weekly_homepage {}

explore: oe_weekly_oe_to_date {}
explore: oe_weekly_planfindermetrics {}
explore: oe_weekly_medicareaccounts {}
explore: oe_weekly_medicaresitewide {}
explore: oe_weekly_homepage {}

explore: oe_oetodate_2022_ua {}
explore: oe_planfinder_2022_ua {}
explore: oe_accounts_2022_ua {}
explore: oe_sitewide_2022_ua {}
explore: oe_homepage_2022_ua {}

explore: preoe_oetodate_2022_ua {}
explore: preoe_planfinder_2022_ua {}
explore: preoe_accounts_2022_ua {}
explore: preoe_sitewide_2022_ua {}
explore: preoe_homepage_2022_ua {}

explore: TEST {}
