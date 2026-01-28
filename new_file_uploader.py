import helper_functions as help_me
import datetime as dt
import os 

AsAtDate = str(dt.datetime.now().date()-dt.timedelta(days=1))

# help.update_swap_rates()

# raw_mri_filepath = 'C:\\Users\\hbeckett\\Documents\\property-cashflows\\20250306 Mri'
# help.upload_raw_mri_files(raw_mri_filepath,effective_date = dt.date(2024,12,31))

# raw_mri_filepath = 'C:\\Users\\hbeckett\\Documents\\property-cashflows\\20251028 MRI (to-load (2526 B1))'
# help.upload_raw_mri_files(raw_mri_filepath,effective_date = dt.date(2025,6,30))

# metrics_filepath = 'C:\\Users\\hbeckett\\Documents\\property-cashflows\\property-metrics'
# help.upload_metrics_file(metrics_filepath,add_on=False)

# summary_metrics_filepath = 'C:\\Users\\hbeckett\\Documents\\property-cashflows'
# help.upload_metrics_summary_file(summary_metrics_filepath, add_on=True)

# help.construct_consolidated_metrics(replace=True)

# help.update_detailed_swap_rates()

cashflows = help_me.generate_contracted_cashflows(AsAtDate)

dv01, cashflows = help_me.calculate_dv01(AsAtDate,input_cashflows=None)

print(dv01)

# cashflows.to_csv("20260120 HS Cashflows.csv")
# dv01.to_csv('20260120 dv01.csv')