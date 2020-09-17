
public class DemoOne {
    public static void main(String[] args) {
        double i = 1599990790000.0 - ((1599990790000.0 - 28800000 + 2000.0) % 2000.0);
        double j = (1599990790000.0 - 28800000 + 2000.0) % 2000;
        System.out.println(i);
        System.out.println(j);
    }
}
