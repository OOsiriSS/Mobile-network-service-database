import java.sql.*;


public class Main {
    private static String url = "jdbc:mysql://localhost:3306/mobilevendor?rewriteBatchedStatements=true&serverTimezone=GMT&useUnicode=true&characterEncoding=UTF-8";
    private static String username = "root";
    private static String password = "root";

    public static void main(String[] args){
        //Mysql connection
        Connection connection;
        Statement statement;

        try{
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.createStatement();

            /**
             * 对某个用户进行套餐的查询（包括历史记录）、订购、退订（考虑立即生效和次月生效）操作
             */
            getPackagesByUser(14, statement);//查询

            Date startTime = new Date(System.currentTimeMillis());
            Date endTime = new Date(startTime.getYear(), startTime.getMonth()+1, startTime.getDate());
            subscribePackageForUser(11, 4, startTime, endTime, statement, connection);//订购

            cancelSubscribeForUser(12, false, connection);//根据订单号退订

            //某个用户在通话情况下的资费生成
            addCallRecord(12, 12, connection);

            //某个用户在使用流量情况下的资费生成
            addFlowRecord(12, 200, true, connection);

            //某个用户月账单的生成
            Date queryDate = new Date(118,9,1);//2018.10.1
            getMonthBill(14, queryDate, connection);

        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public static void getPackagesByUser(int uid, Statement statement){
        long timeMillis1 = System.currentTimeMillis();
        String sql = "select p.pid, p.fee, p.calllimit, p.messagelimit, p.localflowlimit, p.domesticflowlimit, s.starttime, s.endtime from subscribe s, package p where s.uid = '"
                + uid
                + "' and s.pid = p.pid";
        try {
            ResultSet resultSet = statement.executeQuery(sql);

            System.out.println("该用户订购了如下套餐：");
            System.out.println();

            while(resultSet.next()){
                int pid = resultSet.getInt("pid");
                double fee = resultSet.getDouble("fee");
                double call_limit = resultSet.getDouble("calllimit");
                int message_limit = resultSet.getInt("messagelimit");
                double localflow_limit = resultSet.getDouble("localflowlimit");
                double domesticflow_limit = resultSet.getDouble("domesticflowlimit");
                Date startTime = resultSet.getDate("starttime");
                Date endTime = resultSet.getDate("endtime");

                System.out.println("套餐编号：" + pid);
                System.out.println("套餐费用：" + fee + "元");

                if(call_limit > 0)System.out.println("通话时长：" + call_limit + "分钟");
                if(message_limit > 0)System.out.println("短信条数：" + message_limit + "条");
                if(localflow_limit > 0)System.out.println("本地流量：" + localflow_limit/1024.0+ "G");
                if(domesticflow_limit > 0)System.out.println("国内流量：" + domesticflow_limit/1024.0 + "G");

                System.out.println("套餐起始时间：" + startTime.toString());
                System.out.println("套餐结束时间：" + endTime.toString());

                System.out.println("========================");
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        long timeMillis2 = System.currentTimeMillis();
        System.out.print("<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.out.println("用时：" + (timeMillis2 - timeMillis1) + "ms\n");
    }

    public static void subscribePackageForUser(int uid, int pid, Date startTime, Date endTime, Statement statement, Connection connection){
        long timeMillis1 = System.currentTimeMillis();
        String sql1 = "select endtime from subscribe where uid = " + uid + " and pid = " + pid;

        try{
            ResultSet  resultSet = statement.executeQuery(sql1);
            while(resultSet.next()){
                Date end = resultSet.getDate("endtime");
                if (end.after(startTime)){
                    System.out.println("已订购该套餐，请不要重复订购！");
                    long timeMillis2 = System.currentTimeMillis();
                    System.out.print("<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                    System.out.println("用时：" + (timeMillis2 - timeMillis1) + "ms\n");
                    return;
                }
            }
            PreparedStatement preparedStatement = connection.prepareStatement("insert into subscribe (uid, pid, starttime, endtime) values (?,?,?,?)");
            preparedStatement.setString(1, uid+"");
            preparedStatement.setString(2, pid+"");
            preparedStatement.setString(3, startTime.toString());
            preparedStatement.setString(4, endTime.toString());

            preparedStatement.execute();
            System.out.println("订购成功，将于" + startTime + "生效。");

        }catch (SQLException e){
            e.printStackTrace();
        }

        long timeMillis2 = System.currentTimeMillis();
        System.out.print("<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.out.println("用时：" + (timeMillis2 - timeMillis1) + "ms\n");

    }

    public static void cancelSubscribeForUser(int sid, boolean instant, Connection connection){
        long timeMillis1 = System.currentTimeMillis();
        try{
            Statement statement = connection.createStatement();
            String sql = "select endtime from subscribe where sid = " + sid;
            ResultSet resultSet = statement.executeQuery(sql);
            Date endtime = new Date(System.currentTimeMillis());
            while(resultSet.next()){
                endtime = resultSet.getDate("endtime");
            }
            if(new Date(System.currentTimeMillis()).after(endtime)){
                System.out.println("该订阅已过期！");
                long timeMillis2 = System.currentTimeMillis();
                System.out.print("<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                System.out.println("用时：" + (timeMillis2 - timeMillis1) + "ms\n");
                return;
            }
            PreparedStatement preparedStatement = connection.prepareStatement("update subscribe set endtime = ? where sid = ?");
            Date currentDate = new Date(System.currentTimeMillis());
            if(instant){
                preparedStatement.setString(1, currentDate.toString());
            }else {
                Date nextMonth = new Date(currentDate.getYear(), currentDate.getMonth()+1, currentDate.getDate());
                preparedStatement.setString(1, nextMonth.toString());
            }

            preparedStatement.setString(2, sid+"");

            preparedStatement.execute();
            if(instant){
                System.out.println("退订套餐成功，即刻生效！");
            }else{
                System.out.println("退订套餐成功，下月生效！");
            }
            long timeMillis2 = System.currentTimeMillis();
            System.out.print("<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            System.out.println("用时：" + (timeMillis2 - timeMillis1) + "ms\n");

        }catch (SQLException e){
            System.out.println("退订失败。");
            System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            e.printStackTrace();
        }
    }

    public static void addCallRecord(int uid, double calltime, Connection connection) {
        long timeMillis1 = System.currentTimeMillis();
        Date current_date = new Date(System.currentTimeMillis());
        Date monthDate = new Date(current_date.getYear(), current_date.getMonth(), 1);
        try{
            //查询是否已经有该月的记录
            PreparedStatement preparedStatement1 = connection.prepareStatement("select count(*) from bill where starttime = ? and uid = ?");
            preparedStatement1.setString(1, monthDate.toString());
            preparedStatement1.setString(2, uid+"");
            ResultSet resultSet = preparedStatement1.executeQuery();
            resultSet.next();


            if(resultSet.getInt(1) == 0){
                //没有记录
                PreparedStatement preparedStatement2 = connection.prepareStatement("insert into bill (uid, starttime, calltime) value (?, ?, ?)");
                preparedStatement2.setString(1, uid + "");
                preparedStatement2.setString(2, monthDate.toString());
                preparedStatement2.setString(3, calltime + "");
            }else{
                //查询已有记录
                PreparedStatement preparedStatement = connection.prepareStatement("select calltime from bill where starttime = ? and uid = ?");
                preparedStatement.setString(1, monthDate.toString());
                preparedStatement.setString(2, uid+"");
                resultSet = preparedStatement.executeQuery();
                double last_call_time = 0;
                while(resultSet.next()){
                    last_call_time = resultSet.getDouble("calltime");
                }
                calltime += last_call_time;

                //更新
                PreparedStatement preparedStatement2 = connection.prepareStatement("update bill set calltime  = ? where uid = ? and starttime = ?");
                preparedStatement2.setString(1,calltime+"");
                preparedStatement2.setString(2, uid+"");
                preparedStatement2.setString(3, monthDate.toString());
                preparedStatement2.execute();
            }
            System.out.println("通话资费已更新。");
            long timeMillis2 = System.currentTimeMillis();
            System.out.print("<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            System.out.println("用时：" + (timeMillis2 - timeMillis1) + "ms\n");
        }catch (SQLException e){
            e.printStackTrace();
        }

    }

    public static void addFlowRecord(int uid, double flow, boolean ifLocal, Connection connection){
        long timeMillis1 = System.currentTimeMillis();
        Date current_date = new Date(System.currentTimeMillis());
        Date monthDate = new Date(current_date.getYear(), current_date.getMonth(), 1);
        try{
            //查询是否已经有该月的记录
            PreparedStatement preparedStatement1 = connection.prepareStatement("select count(*) from bill where starttime = ? and uid = ?");
            preparedStatement1.setString(1, monthDate.toString());
            preparedStatement1.setString(2, uid+"");
            ResultSet resultSet = preparedStatement1.executeQuery();
            resultSet.next();

            if(resultSet.getInt(1) == 0){
                //没有记录
                PreparedStatement preparedStatement2;
                if(ifLocal){
                    preparedStatement2 = connection.prepareStatement("insert into bill (uid, starttime, localflow) value (?, ?, ?)");
                }else{
                    preparedStatement2 = connection.prepareStatement("insert into bill (uid, starttime, domesticflow) value (?, ?, ?)");
                }
                preparedStatement2.setString(1, uid + "");
                preparedStatement2.setString(2, monthDate.toString());
                preparedStatement2.setString(3, flow + "");
            }else{
                //查询已有记录
                PreparedStatement preparedStatement;
                if(ifLocal){
                    preparedStatement = connection.prepareStatement("select localflow from bill where starttime = ? and uid = ?");
                }else{
                    preparedStatement = connection.prepareStatement("select domexticflow from bill where starttime = ? and uid = ?");
                }
                preparedStatement.setString(1, monthDate.toString());
                preparedStatement.setString(2, uid+"");
                resultSet = preparedStatement.executeQuery();
                double last_flow = 0;
                while(resultSet.next()){
                    if(ifLocal){
                        last_flow = resultSet.getDouble("localflow");
                    }else{
                        last_flow = resultSet.getDouble("domesticflow");
                    }

                }
                flow += last_flow;

                PreparedStatement preparedStatement2 = connection.prepareStatement("update bill set calltime  = ? where uid = ? and starttime = ?");
                if(ifLocal){
                    preparedStatement2 = connection.prepareStatement("update bill set localflow  = ? where uid = ? and starttime = ?");
                }else{
                    preparedStatement2 = connection.prepareStatement("update bill set domesticflow  = ? where uid = ? and starttime = ?");
                }
                preparedStatement2.setString(1,flow+"");
                preparedStatement2.setString(2, uid+"");
                preparedStatement2.setString(3, monthDate.toString());
                preparedStatement2.execute();
            }
            System.out.println("流量资费已更新。");
            long timeMillis2 = System.currentTimeMillis();
            System.out.print("<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            System.out.println("用时：" + (timeMillis2 - timeMillis1) + "ms\n");
        }catch (SQLException e){
            e.printStackTrace();
        }

    }

    public static void getMonthBill(int uid, Date date, Connection connection){
        long timeMillis1 = System.currentTimeMillis();
        try {
            //查询该月账单
            PreparedStatement preparedStatement1 = connection.prepareStatement("select * from bill b where b.uid = ? and b.starttime = ?");
            preparedStatement1.setString(1,uid+"");
            preparedStatement1.setString(2, date.toString());
            ResultSet resultSet = preparedStatement1.executeQuery();

            double calltime = 0;
            int message = 0;
            double localflow = 0;
            double domesticflow = 0;

            while(resultSet.next()){
                calltime = resultSet.getDouble("calltime");
                message = resultSet.getInt("message");
                localflow = resultSet.getDouble("localflow");
                domesticflow = resultSet.getDouble("domesticflow");
            }

            //查询订购的套餐
            PreparedStatement preparedStatement2 = connection.prepareStatement("select p.fee, p.calllimit, p.messagelimit, p.localflowlimit, p.domesticflowlimit from subscribe s, package p where s.uid = ? and s.pid = p.pid and s.endtime > ?");
            preparedStatement2.setString(1, uid+"");
            preparedStatement2.setString(2, date.toString());
            resultSet = preparedStatement2.executeQuery();

            double fee = 0;
            double call_limit = 0;
            int message_limit = 0;
            double localflow_limit = 0;
            double domesticflow_limit = 0;

            while(resultSet.next()){
                fee += resultSet.getDouble("fee");
                call_limit += resultSet.getDouble("calllimit");
                message_limit += resultSet.getInt("messagelimit");
                localflow_limit += resultSet.getDouble("localflowlimit");
                domesticflow_limit += resultSet.getDouble("domesticflowlimit");
            }

            double package_fee = fee;

            double call_fee = (calltime - call_limit) * 0.5;
            double message_fee = (message - message_limit) * 0.1;
            double localflow_fee = (localflow - localflow_limit) * 2;
            double domestic_fee = (domesticflow - domesticflow_limit) * 5;

            if(call_fee > 0){
                fee += call_fee;
            }else{
                call_fee = 0;
            }

            if(message_fee > 0){
                fee += message_fee;
            }else{
                message_fee = 0;
            }

            if(localflow_fee > 0){
                fee += localflow_fee;
            }else{
                localflow_fee = 0;
            }

            if(domestic_fee > 0){
                fee += domestic_fee;
            }else{
                domestic_fee= 0;
            }

            System.out.println("您好，以下是您" + (date.getYear()+1900) + "年" + (date.getMonth()+1) + "月的账单：");
            System.out.println("您的总消费为：" + fee + "元");
            System.out.println("套餐费用为：" + package_fee + "元\n");
            System.out.println("套餐外费用");
            System.out.println("通话费用：" + call_fee + "元");
            System.out.println("短信费用：" + message_fee + "元");
            System.out.println("本地流量费用：" + localflow_fee + "元");
            System.out.println("国内流量费用：" + domestic_fee + "元");

            long timeMillis2 = System.currentTimeMillis();
            System.out.print("<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            System.out.println("用时：" + (timeMillis2 - timeMillis1) + "ms\n");

        }catch (SQLException e){
            e.printStackTrace();
        }
    }
}
