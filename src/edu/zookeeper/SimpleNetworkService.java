package edu.zookeeper;

import java.io.OutputStream;
import java.net.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleNetworkService {

	private int port;
	private String nome;
	private AtomicBoolean active = new AtomicBoolean(false);

	public SimpleNetworkService(int port, String nome) {
		this.port = port;
		this.nome = nome;
	}

	public void listem() {
		try {
			active.set(true);
			ServerSocket server = new ServerSocket(port);
			while (active.get()) {
				Socket socket = server.accept();
				OutputStream out = socket.getOutputStream();
				out.write(("Você esta conectado no servico " + nome + "\n").getBytes());
				out.close();
				socket.close();
			}
			server.close();
		} catch (Exception ex) {
			
		}

	}

}
