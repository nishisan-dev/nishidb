package dev.nishisan.nishidb.jdbc.wire;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * O corpo de uma resposta com prazo em cada espera por bytes.
 *
 * <p>O {@code HttpRequest.timeout} do JDK para de valer quando os cabeçalhos
 * chegam (conferido no JDK 21 e no 25: um corpo que flui por mais tempo que o
 * prazo é lido inteiro, e um corpo que para depois dos cabeçalhos espera para
 * sempre). É esta classe que cobre o resto. Cada leitura agenda um alarme;
 * se a leitura não volta antes dele, o alarme FECHA o stream do HttpClient,
 * que é o que destrava um {@code read} bloqueado nele — interromper a thread
 * não destrava. A leitura que acordou assim lança a exceção que a política
 * do {@link RequestTimeout} montou.
 *
 * <p>O alarme é por leitura, e não um relógio da resposta inteira, porque o
 * que o prazo mede é o SILÊNCIO do servidor: o tempo que a aplicação passa
 * processando linhas entre duas leituras não está em espera nenhuma. Com
 * {@code deadlineNanos}, a espera também não passa do instante absoluto — é
 * o modo dos documentos pequenos, em que o prazo é da requisição inteira.
 *
 * <p>O custo é um agendamento por leitura, e uma leitura do
 * {@link LeitorNdjson} traz até 64 KiB: em escala de linha, nada. Sem prazo,
 * o corpo nem é embrulhado.
 */
final class TimedInputStream extends FilterInputStream {

    /**
     * Um relógio para o driver inteiro, numa thread daemon que morre ociosa.
     *
     * <p>Daemon para não segurar o desligamento da JVM; ociosa morre para não
     * prender o classloader do driver numa ferramenta que o descarrega (o
     * DBeaver troca de versão de driver sem reiniciar). Cancelar remove o
     * alarme da fila: sem isso, um stream longo deixaria um alarme cancelado
     * por leitura esperando a hora dele.
     */
    private static final ScheduledThreadPoolExecutor TIMER = newTimer();

    private final RequestTimeout timeout;
    private final long waitNanos;
    private final boolean hasDeadline;
    private final long deadlineNanos;

    private final Object lock = new Object();
    /** Há uma leitura em curso — o alarme só fecha o stream se houver. */
    private boolean reading;
    private boolean expired;
    /**
     * A exceção do estouro, montada uma vez: a política pode ter efeito
     * colateral. Só a thread que lê a toca.
     */
    private SQLException failure;

    /**
     * @param deadlineNanos instante absoluto de {@link System#nanoTime()} que
     *                      nenhuma espera ultrapassa, ou {@code null}
     */
    TimedInputStream(InputStream in, RequestTimeout timeout, Long deadlineNanos) {
        super(in);
        if (!timeout.isSet()) {
            throw new IllegalArgumentException("corpo embrulhado sem prazo");
        }
        this.timeout = timeout;
        this.waitNanos = TimeUnit.MILLISECONDS.toNanos(timeout.millis());
        this.hasDeadline = deadlineNanos != null;
        this.deadlineNanos = hasDeadline ? deadlineNanos : 0L;
    }

    @Override
    public int read() throws IOException {
        return (int) guarded(() -> in.read());
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return (int) guarded(() -> in.read(b, off, len));
    }

    @Override
    public long skip(long n) throws IOException {
        return guarded(() -> in.skip(n));
    }

    private interface BlockingRead {
        long run() throws IOException;
    }

    private long guarded(BlockingRead read) throws IOException {
        long wait = waitNanos;
        if (hasDeadline) {
            wait = Math.min(wait, deadlineNanos - System.nanoTime());
        }
        boolean alreadyExpired;
        synchronized (lock) {
            alreadyExpired = expired || wait <= 0;
            if (alreadyExpired) {
                expired = true;
            } else {
                reading = true;
            }
        }
        if (alreadyExpired) {
            throw expiredException();
        }
        ScheduledFuture<?> alarm = TIMER.schedule(this::expire, wait, TimeUnit.NANOSECONDS);
        try {
            return read.run();
        } catch (IOException e) {
            boolean timedOut;
            synchronized (lock) {
                timedOut = expired;
            }
            if (timedOut) {
                ResponseTimeoutException t = expiredException();
                t.addSuppressed(e);
                throw t;
            }
            throw e;
        } finally {
            alarm.cancel(false);
            synchronized (lock) {
                reading = false;
            }
        }
    }

    /** Roda no relógio: fecha o stream para acordar a leitura bloqueada. */
    private void expire() {
        synchronized (lock) {
            if (!reading || expired) {
                return;
            }
            expired = true;
        }
        try {
            in.close();
        } catch (IOException e) {
            // O stream já está sendo abandonado: a falha de fechá-lo não tem
            // a quem ser entregue, e a leitura que esperava recebe o estouro
            // de qualquer forma.
        }
    }

    /**
     * Só a thread que lê chama isto — o stream não é de uso concorrente —, e
     * fora do {@code lock}: a política pode derrubar a conexão inteira.
     */
    private ResponseTimeoutException expiredException() {
        if (failure == null) {
            failure = timeout.onExpiry().onExpiry("o servidor ficou " + timeout.millis()
                    + " ms sem mandar bytes da resposta");
        }
        return new ResponseTimeoutException(failure);
    }

    private static ScheduledThreadPoolExecutor newTimer() {
        ScheduledThreadPoolExecutor t = new ScheduledThreadPoolExecutor(1, r -> {
            Thread th = new Thread(r, "nishidb-jdbc-timeout");
            th.setDaemon(true);
            return th;
        });
        t.setRemoveOnCancelPolicy(true);
        t.setKeepAliveTime(30, TimeUnit.SECONDS);
        t.allowCoreThreadTimeOut(true);
        return t;
    }
}
