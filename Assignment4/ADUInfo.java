public class ADUInfo{
	String IP;
	long sent;
	long rec;
	public ADUInfo(String IP,long s, long r){
			this.IP=IP;
			sent=s;
			rec=rec;
	}
	public long getSent(){
		return sent;
	}
	public long getRec(){
		return rec;
	}
	public String getIP(){
		return IP;
	}
	@Override
	public String toString(){
		return sent+"\t"+rec;
	}
}