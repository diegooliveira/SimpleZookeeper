package edu.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class CaminhoZookeeper {

    public static void criaSeNaoExiste(String caminho, ZooKeeper zk) {
        if (!caminho.startsWith("/"))
            new RuntimeException("O caminho deve começar com '/'");
        
        String[] partesCaminho = caminho.split("/");
        for (int indice = 1; indice < partesCaminho.length; indice++) {
            String caminhoParcial = montaCaminhoAte(indice, partesCaminho);
            try {
                if (zk.exists(caminhoParcial, false) == null)
                    zk.create(caminhoParcial, Constantes.NENHUM_CONTEUDO, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
            } catch (KeeperException | InterruptedException e) {
            }
        }
    }

    private static String montaCaminhoAte(int indice, String[] partesCaminho) {
        StringBuilder sbCaminhoParcial = new StringBuilder();
        for (int aux = 1; aux <= indice; aux++) {
            sbCaminhoParcial.append("/").append(partesCaminho[aux]);
        }
        return sbCaminhoParcial.toString();
    }

}
