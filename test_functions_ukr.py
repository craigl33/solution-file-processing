import solution_file_processing as sfp

# Initialize config with toml file
config = sfp.SolutionFilesConfig('config_archive/ukraine/UKR.toml')

# sfp.plots.create_plot_1a(config)
# sfp.plots.create_plot_1b(config)
# sfp.plots.create_plot_2(config)
# sfp.plots.create_plot_3(config)
# sfp.plots.create_plot_6(config)
# sfp.plots.create_plot_7(config)

# sfp.summary.create_output_10(config)
# sfp.plots._get_plot_1_variables(config)
# sfp.timeseries.create_output_11(config)

# sfp.timeseries.create_output_6(config)
sfp.plots.create_plot_1a(config)
sfp.plots.create_plot_1b(config)
sfp.plots.create_plot_1c(config, toi='15-12')
# sfp.plots.create_plot_2(config)
# sfp.plots.create_plot_3(config)
# sfp.plots.create_plot_6(config)
# sfp.plots.create_plot_7(config)