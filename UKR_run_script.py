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
# sfp.plots.create_plot_2_summary(config, plot_vars=config.QUICK_PLOTS, plot_name='quick')
sfp.plots.create_plot_2_summary(config)
sfp.plots.create_plot_2b_ref_plots(config, ref_m='2025')
sfp.plots.create_plot_2b_ref_plots(config, ref_m='2021')
# sfp.plots.create_plot_2b_ref_plots(config, ref_m='2030_cPN_eSG')

sfp.plots.create_plot_1a_overall(config)
sfp.plots.create_plot_1a_overall_models(config)
# sfp.plots.create_plot_1b(config)
# sfp.plots.create_plot_2b_ref_plots(config, ref_m='2030-BR_cPN_eSG')
# sfp.plots.create_plot_3(config)
sfp.plots.create_plot_6_ldc_and_line_plots(config)

sfp.plots.create_plot_8_services(config)
# sfp.plots.create_plot_4_costs(config) #unimplemented
sfp.plots.create_plot_5_undispatched_tech(config)
sfp.plots.create_plot_7_co2_savings(config)
sfp.plots.create_plot_9_av_cap(config)
sfp.plots.create_plot_10a_vre_gen_by_model(config)
sfp.plots.create_plot_10b_vre_cf_by_model(config)
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

