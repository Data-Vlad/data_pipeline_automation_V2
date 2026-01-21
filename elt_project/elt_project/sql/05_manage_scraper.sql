UPDATE elt_pipeline_configs
SET
    -- 1. Set the parser to use the Selenium logic
    parser_function = 'generic_selenium_scraper',
    
    -- 2. Define the scraping workflow JSON
    scraper_config = '{
        "driver_options": {
            "headless": true
        },
        "login_url": "https://example.com/login",
        "actions": [
            {
                "type": "find_and_fill",
                "selector": "id",
                "selector_value": "username_field_id",
                "value_env_var": "MY_SITE_USERNAME"
            },
            {
                "type": "find_and_fill",
                "selector": "id",
                "selector_value": "password_field_id",
                "value_env_var": "MY_SITE_PASSWORD"
            },
            {
                "type": "find_and_fill_totp",
                "selector": "id",
                "selector_value": "totp_input_id",
                "totp_secret_env_var": "MY_SITE_TOTP_SECRET"
            },
            {
                "type": "click",
                "selector": "id",
                "selector_value": "login_button_id"
            },
            {
                "type": "wait_for_element",
                "selector": "id",
                "selector_value": "dashboard_element_id",
                "timeout": 15
            }
        ],
        "data_extraction": [
            {
                "target_import_name": "my_scraped_data",
                "method": "html_table",
                "table_index": 0,
                "output_file": "C:/Pipelines/Incoming/my_scraped_data.csv"
            }
        ]
    }'
WHERE import_name = 'your_import_name'; -- Replace with the specific import you are configuring
