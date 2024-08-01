"""
This is a template run script for the solution file processing package. Do not edit this file, but copy it to the
main directory and edit it there.
"""

import solution_file_processing as sfp

# Initialize config with toml file
config = sfp.SolutionFilesConfig('config_files/ukraine/UKR.toml')  # e.g. 'config_archive/vanilla_dev/IDN.toml'

# Only when running the first time
# config.install_dependencies()
config.convert_solution_files_to_h5()

# # # # Create plots (stored in output/plots)

# sfp.plots.create_plot_2(config)
sfp.plots.create_plot_2b_ref_plots(config, ref_m='2023')
sfp.plots.create_plot_2b_ref_plots(config, ref_m='2030-BR_cPN')

sfp.plots.create_plot_1a(config)
sfp.plots.create_plot_1b(config)
# sfp.plots.create_plot_2b_ref_plots(config, ref_m='2030-BR_cPN_eSG')
# sfp.plots.create_plot_3(config)
sfp.plots.create_plot_6_ldc(config)

sfp.plots.create_plot_8_services(config)
# sfp.plots.create_plot_4_costs(config) #unimplemented
sfp.plots.create_plot_5_undispatched_tech(config)
sfp.plots.create_plot_7_co2_savings(config)
sfp.plots.create_plot_9_av_cap(config)
sfp.plots.create_plot_10_ts_by_model(config)
# sfp.plots.create_plot_2b_ref_plots(config, ref_m='2030-IR_cPN_eSG')


# # # Create summary outputs (stored in output/summary)
sfp.summary.create_output_1(config)
sfp.summary.create_output_2(config)
sfp.summary.create_output_3(config)
sfp.summary.create_output_4(config)
sfp.summary.create_output_5(config)
sfp.summary.create_output_6(config)
sfp.summary.create_output_7(config)
sfp.summary.create_output_8(config)
sfp.summary.create_output_9(config)
sfp.summary.create_output_10(config)
sfp.summary.create_output_11(config)
sfp.summary.create_output_12(config)
sfp.summary.create_output_13(config)

# # # Create timeseries outputs (stored in output/timeseries)
# sfp.timeseries.create_output_1(config)
# # sfp.timeseries.create_output_2(config)
# # sfp.timeseries.create_output_3(config)
# # sfp.timeseries.create_output_4(config)
# # sfp.timeseries.create_output_5(config)
# # sfp.timeseries.create_output_6(config)
# sfp.timeseries.create_output_7(config)
# # sfp.timeseries.create_output_8i(config)
# # sfp.timeseries.create_output_8ii(config)
# sfp.timeseries.create_output_10(config)
# sfp.timeseries.create_output_11(config)

