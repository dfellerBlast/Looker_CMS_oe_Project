connection: "cms_medicare_ga360"

include: "/views/*.view.lkml"

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
