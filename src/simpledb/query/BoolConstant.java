package simpledb.query;

/**
 * The class that wraps Java strings as database constants.
 * @author Edward Sciore
 */
public class BoolConstant implements Constant {
   private boolean bool;
   
   /**
    * Create a constant by wrapping the specified boolan.
    * @param s the Boolean value
    */
   public BoolConstant(Boolean s) {
      bool = (boolean)s;
   }
   
   /**
    * Unwraps the boolean and returns it.
    * @see simpledb.query.Constant#asJavaVal()
    */
   public Boolean asJavaVal() {
      return bool;
   }
   
   public boolean equals(Object obj) {
      BoolConstant sc = (BoolConstant) obj;
      return sc.bool == bool;
   }
   
   public int compareTo(Constant c) {
      BoolConstant sc = (BoolConstant) c;
      return sc.asJavaVal().compareTo(bool);
   }
   
   public int hashCode() {
      return asJavaVal().hashCode();
   }
   
   public String toString() {
      return asJavaVal().toString();
   }
}
