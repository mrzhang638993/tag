 // 1. 初始化上游变量
        var initDateStr = "2019-01-01";
        var locale = new java.util.Locale("en", "us");
        var calendar = java.util.Calendar.getInstance();
        var sdf = new java.text.SimpleDateFormat("yyyy-MM-dd", locale);
        var initDate  = sdf.parse(initDateStr);

        calendar.setTime(initDate);
        calendar.add(java.util.Calendar.DAY_OF_MONTH, setup);

        var curDate = calendar.getTime();

        // 2. 生成维度数据
        // 2.1 生成代理键
        sdf.applyPattern("yyyyMMdd");
        var date_key = sdf.format(curDate);

        // 2.2 年-月-日
        sdf.applyPattern("yyyy-MM-dd");
        var date_value = sdf.format(curDate);

        // 2.3 当年的第几天
        var day_in_year = calendar.get(java.util.Calendar.YEAR) + "";

        // 2.4 当月的第几天
        var day_in_month = calendar.get(java.util.Calendar.DAY_OF_MONTH) + "";

        // 2.5 是否为当月的第一天
        var is_first_day_in_month = day_in_month.equals("1") ? "y":"n";

        // 2.6 是否为当月最后一天
        // 1. 加一天
        calendar.add(java.util.Calendar.DAY_OF_MONTH, 1);

        var is_last_day_in_month = calendar.get(java.util.Calendar.DAY_OF_MONTH) == 1 ? "y":"n";
        // 2. 减回去
        calendar.add(java.util.Calendar.DAY_OF_MONTH, -1);

        // 2.7 星期
        var weekday = (calendar.get(java.util.Calendar.DAY_OF_WEEK) - 1) + "";
        if(weekday.equals("0")) {
            weekday = "7";
        }

        // 2.8 月的第几个星期
        sdf.applyPattern("W");
        var week_in_month = sdf.format(curDate);

        // 2.9 是否周一
        var is_first_day_in_week = calendar.get(java.util.Calendar.DAY_OF_WEEK) == java.util.Calendar.MONDAY ? "y":"n";

        // 2.10 是否休息日
        var is_dayoff = "n";

        // 2.11 是否工作日
        var is_workday = "n";

        // 2.12 是否国家法定节假日
        var is_holiday = "n";
        // 2.12 国家法定节假日获取URL
        var holiday_url = "http://timor.tech/api/holiday/info/" + date_value;

        // 2.13 日期类型
        var date_type = "workday";

        // 2.14 月份
        sdf.applyPattern("MM");
        var month_number = sdf.format(curDate);

        // 2.15 年份
        sdf.applyPattern("yyyy");
        var year = sdf.format(curDate);

        // 2.16 年份-月份
        sdf.applyPattern("yyyy-MM");
        var year_month_number = sdf.format(curDate);

        // 2.16 季度名称、季度、年季度
        var quarter_name = "";
        var quarter_number = "";
        var year_quarter = "";

        switch (calendar.get(java.util.Calendar.MONTH)) {
            case java.util.Calendar.FEBRUARY:
            case java.util.Calendar.JANUARY:
            case java.util.Calendar.MARCH:
                quarter_name = "Q1";
                quarter_number = "1";
                year_quarter = year + "-" + quarter_name;
                break;
            case java.util.Calendar.APRIL:
            case java.util.Calendar.MAY:
            case java.util.Calendar.JUNE:
                quarter_name = "Q2";
                quarter_number = "2";
                year_quarter = year + "-" + quarter_name;
                break;
            case java.util.Calendar.JULY:
            case java.util.Calendar.AUGUST:
            case java.util.Calendar.SEPTEMBER:
                quarter_name = "Q3";
                quarter_number = "3";
                year_quarter = year + "-" + quarter_name;
                break;
            case java.util.Calendar.OCTOBER:
            case java.util.Calendar.NOVEMBER:
            case java.util.Calendar.DECEMBER:
                quarter_name = "Q4";
                quarter_number = "4";
                year_quarter = year + "-" + quarter_name;
                break;
        }