import java.util.Date;

public class TestTime {
    public static void main(String[] args) {
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
//        String format = dateFormat.format(new Date());
//        System.out.println(format+"\t"+new Date());

        Date date = new Date();
        System.out.println(date.toString());
        long time = new Date().getTime();
        System.out.println(time);
    }
}
