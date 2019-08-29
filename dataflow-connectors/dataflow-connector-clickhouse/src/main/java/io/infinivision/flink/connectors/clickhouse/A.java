package io.infinivision.flink.connectors.clickhouse;

import ru.yandex.clickhouse.util.ClickHouseArrayUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

public class A {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		String s = ClickHouseArrayUtil.arrayToString(new String[]{"", "\"[]2", "3", "4"}, true);
		System.out.println(s);
		for (boolean b : new boolean[2]) {
			System.out.println(b);

		}

		//language=JSON
//		String ls = "[\"fafjal\",\"fjal\\\"\",\"fafa\"]";
//		System.out.println(ls);
		// ["fkfjfka\"-aj,fka","kff,ka'"]
		Class.forName(ClickHouseTableFactory.DRIVERNAME);
		Connection connection = DriverManager.getConnection("jdbc:clickhouse://10.126.144.141:8123/default?socket_timeout=300000", "default", "infinivision2019");

		PreparedStatement preparedStatement = connection.prepareStatement("insert into cdp_orders_test_2 (order_id, order_complete_time, order_year, order_month, is_order_holiday, holiday_name, week_tag, day_tag, order_daypart, order_reserve_time, order_status, be_type, order_type, order_platform, order_channel, eating_type, uscode, store_name, store_ta_type, city_name, city_tier, market, ownership, is_hub_mds, is_hub_bk, is_hub_kfc, bk_num_1km, kfc_num_1km, bk_num_2km, kfc_num_2km, bk_num_3km, kfc_num_3km, order_latitude, order_longitude, order_distance_store, delivery_add_type, is_digital_channel_bm_kfc, is_digital_channel_bm_mcd, is_mem_order, payment_platform, payment_channel, is_mds_order, is_breakfast_order, is_snacks_order, is_sides_order, is_beverage_order, is_dessert_order, is_beef_order, is_spicy_chicken_order, is_non_spicy_chicken_order, is_sharing_order, is_family_order, is_snacking_order, is_value_order, is_angus_order, is_first_buy, is_first_buy_value, is_first_buy_mds, is_first_buy_breakfast, is_first_buy_family, is_first_buy_snacking, is_first_buy_angus, is_first_repeat_buy, is_first_repeat_buy_value, is_first_repeat_buy_mds, is_first_repeat_buy_breakfast, is_first_repeat_buy_family, is_first_repeat_buy_snacking, is_first_repeat_buy_angus, is_repeat_buy, is_repeat_buy_value, is_repeat_buy_mds, is_repeat_buy_breakfast, is_repeat_buy_family, is_repeat_buy_snacking, is_repeat_buy_angus, is_value_addon, value_addon_sales, is_family_addon, family_addon_sales, is_snacking_addon, snacking_addon_sales, is_angus_addon, angus_addon_sales, is_mds_snacks, is_mds_sides, is_mds_beverage, is_mds_dessert, is_mds_beef, is_mds_spicy_chicken, is_mds_non_spicy_chicken, is_mds_sharing, is_mds_breakfast, mid, mid_seq, is_member, membercode, is_newcust, is_newcust_after_member, order_total_sales, order_net_sales, order_product_sales, order_delivery_fee, order_net_sales_range, order_item_num, order_item_range, guest_num, source_table, dt, `detail.is_p_item`, `detail.p_item_code`, `detail.p_item_name`, `detail.item_code`, `detail.item_name`, `detail.item_level1_name`, `detail.item_level2_name`, `detail.item_level3_name`, `detail.item_level4_name`, `detail.item_level5_name`, `detail.item_level7_name`, `detail.item_level8_name`, `detail.is_item_coupon`, `detail.coupon_code`, `detail.is_family_item`, `detail.is_angus_item`, `detail.is_snacking_item`, `detail.is_value_item`, `detail.is_snacks_item`, `detail.is_sides_item`, `detail.is_beverage_item`, `detail.is_dessert_item`, `detail.is_beef_item`, `detail.is_spicy_chicken_item`, `detail.is_non_spicy_chicken_item`, `detail.is_sharing_item`, `detail.item_quantity`, `detail.item_price`, `detail.item_net_price`, `detail.source_table`, `detail.dt`, member_origin_channel, mobile_province, mobile_city, first_buy_province, first_buy_city, first_buy_market, first_buy_store) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		String[] ss = {
				"2654051f-06df-11e8-9f90-da304c76e067","2018-02-01 00:00:00","2018","2","0","NA","weekday","星期四","Breakfast","2018-02-01 07:33:32","1","MDS","线上","3PO","ELEM","Delivery","1990167","泗洪麦当劳花园口餐厅 SIHONG HUAYUANKOU","Shop","宿迁市","Tier 4","江苏","CL","1","0","0","0","0","0","0","0","0","33.46","118.24","0","Home","0","0","0","其他","O2O支付","1","0","0","0","1","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","1","0","1","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","1","0","0","0","0","0","0","0fd0819a-c7d6-3a8a-902b-ef75d98d5b50","413838778","1","NA","0","0","32","21.7","0","9","30","0","0","0","1001","2018-02-01","[0,0,0,0,0]","['1337','1337','1337','1337','7030']","['MDS双层板烧麦满分薯饼套餐','MDS双层板烧麦满分薯饼套餐','MDS双层板烧麦满分薯饼套餐','MDS双层板烧麦满分薯饼套餐','订餐费-MDS']","['3519','4825','4989','1337','7030']","['大杯鲜煮咖啡','脆薯饼','双层原味板烧麦满分','MDS双层板烧麦满分薯饼套餐','订餐费-MDS']","['Beverage','Bfst Side','Breakfast Meal Set','Breakfast Meal Set','Non-Product']","['Hot ','Bfst Side','Bfst 2 Item Combo - RMB10','EVB','Non-Product']","['Coffee','Hashbrown','Muffin','Double PGCS muffin','Non-Product']","['Non-McCafe Coffee','Hashbrown','Chicken Muffin','Regular','Non-Product']","['Large','1pc','Regular','Single Size','Single Size']","['','','','','']","['','','','','']","[0,0,0,0,0]","['','','','','']","[0,0,0,0,0]","[0,0,0,0,0]","[0,0,0,0,0]","[0,0,0,0,0]","[0,0,0,0,0]","[0,0,0,0,0]","[1,0,0,0,0]","[0,0,0,0,0]","[0,0,0,0,0]","[0,0,0,0,0]","[0,0,0,0,0]","[0,0,0,0,0]","[1,1,1,1,1]","[11,0,0,12,9]","[0,0,0,0,0]","['','','','','']","['0000-00-00','0000-00-00','0000-00-00','0000-00-00','0000-00-00']","NA","NA","NA","NA","NA","NA","NA"
		};
		System.out.println(new Date());
		long prev = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			for (int j = 0; j < ss.length; j++) {
				preparedStatement.setString(j+1 , ss[j]);
			}
			preparedStatement.addBatch();
		}
		System.out.println(System.currentTimeMillis()-prev);
		System.out.println(new java.util.Date());
	}
}
