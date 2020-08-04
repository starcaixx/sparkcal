package com.lb.sparktest.bean

class Bean {


}



case class UserState(id: String, if_first_order: String)

case class OrderInfo(
                      id: Long,
                      province_id: Long,
                      order_status: String,
                      user_id: Long,
                      final_total_amount: Double,
                      benefit_reduce_amount: Double,
                      original_total_amount: Double,
                      feight_fee: Double,
                      expire_time: String,
                      create_time: String,
                      operate_time: String,
                      var create_date: String,
                      var create_hour: String,
                      var if_first_order:String,

                      var province_name:String,
                      var province_area_code:String,

                      var user_age_group:String,
                      var user_gender:String

                    )

case class DauInfo(mid: String, dt: String, ts: Long)
case class ProvinceInfo(id: String, name: String, region_id: String, area_code: String)
case class WatchUserInfo(uid: String, regin: String, cid: String, cpid: String, cosid: String, wsize: Long, ts: Long, date: String)