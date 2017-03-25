public class ADUInfo{
	long sent;
	long rec;
	public ADUInfo(long s, long r){
			sent=s;
			rec=rec;
	}
	public long getSent(){
		return sent;
	}
	public long getRec(){
		return rec;
	}
	@Override
	public String toString(){
		return sent+"\t"+rec;
	}
}