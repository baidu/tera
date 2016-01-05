set -x

sh some_script_to_start_tera_cluster

python run.py my_conf_file

sh some_script_to_send_report --report_file ../tmp/mail_report
