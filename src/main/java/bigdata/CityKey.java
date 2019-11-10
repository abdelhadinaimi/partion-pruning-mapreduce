package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CityKey implements WritableComparable<CityKey>{
	
	private Text country = new Text();
	private Text name = new Text();
	
	public Text getContinentNumber() {
		return country;
	}

	public void setContinentNumber(String country) {
		this.country.set(country);
	}

	public Text getName() {
		return name;
	}

	public void setName(String name) {
		this.name.set(name);
	}
	
	public void write(DataOutput out) throws IOException {
		country.write(out);
		name.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		country.readFields(in);
		name.readFields(in);
	}

	public int compareTo(CityKey o) {
		int c = country.compareTo(o.country);
		return c == 0 ? name.compareTo(o.getName()) : c;
	}
	
	@Override
	public String toString() {
		return country.toString() + "\t" + name.toString();
	}
	
	@Override
	public int hashCode() {
		return toString().hashCode();
	}
	
}
