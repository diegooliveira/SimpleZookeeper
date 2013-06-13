package edu.zookeeper;

import java.util.List;
import org.apache.zookeeper.*;

public class SimpleClient {

	public static void main(String args[]) {
		new SimpleClient("localhost:2181");
	}

	public SimpleClient(String endereco) {
		try {
			ZooKeeper zk = new ZooKeeper(endereco, 10000, null);
			List<String> filhos = zk.getChildren("/", false);
			for (String path : filhos)
				System.out.println(path);
			zk.close();
		} catch (Exception ex) {
			throw new RuntimeException(
					"Erro ao se conectar no Zookeeper, endereço fornecido ["
							+ endereco + "]", ex);
		}
	}
}