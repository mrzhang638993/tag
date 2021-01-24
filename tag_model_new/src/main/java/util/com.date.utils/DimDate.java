package util.com.date.utils;

import java.text.ParseException;
import java.util.Calendar;

/**
 * 节假日的工具类生成类数据信息
 * */
public class DimDate {
    public static void main(String[] args) throws ParseException {
        //接收外部参数：seqDay
        int seqDay = 0;
        //初始化日期值：2019-01-01
        String initDateStr = "2019-01-01";
        //获取日历对象：java.util.Calendar,并指定为2000-01-01
        final java.util.Calendar calendar = java.util.Calendar.getInstance();
        final java.text.SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd");
        final java.util.Date initDate = dateFormat.parse(initDateStr);
        calendar.setTime(initDate);
        //增加seqDay天数后输出新的日期值
        calendar.add(java.util.Calendar.DAY_OF_YEAR, seqDay);

        //获取新 的日期值
        final java.util.Date newDate = calendar.getTime();
        //date_value
        final String date_value = dateFormat.format(newDate);
        dateFormat.applyPattern("yyyyMMdd");
        //date_key
        final String date_key = dateFormat.format(newDate);

        //day_in_year
        final int day_in_year = calendar.get(Calendar.DAY_OF_YEAR);
        //day_in_month
        final int day_in_month = calendar.get(Calendar.DAY_OF_MONTH);
        //is_first_day_in_month
        String is_first_day_in_month = "n";
        if (day_in_month == 1) {
            is_first_day_in_month = "y";
        }

        //is_last_day_in_month
        String is_last_day_in_month = "n";
        //判断是否是这个月的第一天改为判断是否是下个月的第一天
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        final int next_day_in_month = calendar.get(Calendar.DAY_OF_MONTH);
        if (next_day_in_month == 1) {
            is_last_day_in_month = "y";
        }
        //记得把上面加的1减去
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        //weekday
        int weekday = (calendar.get(Calendar.DAY_OF_WEEK) - 1);
        System.out.println(weekday);
        //星期日：1，星期日：7
        if (weekday == 0) {
            weekday = 7;
        }
        //week_in_month
        //保证时间正确需要先减一天，结合上面的考虑
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        final int week_in_month = calendar.get(Calendar.WEEK_OF_MONTH);
        System.out.println("week_in_month:" + week_in_month);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        //is_first_day_in_week
        String is_first_day_in_week = "n";
        if (weekday == 1) {
            is_first_day_in_month = "y";
        }
        String is_dayoff = "n";
        String is_workday = "n";
        String is_holiday = "n";
        String date_type = "workday";
        //定义查询的url
        String holiday_url = "http://timor.tech/api/holiday/info/" + date_value;
        //month_number
        dateFormat.applyPattern("MM");
        final String month_number = dateFormat.format(newDate);
        //year
        dateFormat.applyPattern("yyyy");
        final String year = dateFormat.format(newDate);

        //year_month_number
        String year_month_number = year + "-" + month_number;
        //quarter_name
        String quarter_name = "";
        //quarter_number
        String quarter_number = "";
        //year_quarter
        String year_quarter = "";
        switch ((calendar.get(Calendar.MONTH) + 1)) {
            case 1:
            case 2:
            case 3:
                quarter_name = "Q1";
                quarter_number = "1";
                year_quarter = year + "-" + quarter_name;
                break;
            case 4:
            case 5:
            case 6:
                quarter_name = "Q2";
                quarter_number = "2";
                year_quarter = year + "-" + quarter_name;
                break;
            case 7:
            case 8:
            case 9:
                quarter_name = "Q3";
                quarter_number = "3";
                year_quarter = year + "-" + quarter_name;
                break;
            case 10:
            case 11:
            case 12:
                quarter_name = "Q4";
                quarter_number = "4";
                year_quarter = year + "-" + quarter_name;
                break;
        }

        System.out.println(date_value);

    }
}
