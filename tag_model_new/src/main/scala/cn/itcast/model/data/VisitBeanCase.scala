package cn.itcast.model.data

case class VisitBeanCase(
												guid      :String,
											session			: String,
										 remote_addr	: String,
										 inTime				: String,
										 outTime			: String,
										 inPage			  : String,
										 outPage			: String,
										 referal			: String,
										 pageVisits		: Int )


