package hello

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.http.MediaType.APPLICATION_JSON
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.body
import org.springframework.web.reactive.function.server.router
import reactor.core.publisher.Mono
import com.google.api.core.ApiFuture;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import org.json.JSONArray;
import org.json.JSONObject;
 
import java.io.IOException;
import java.time.Instant;

@SpringBootApplication
class KotlinApplication {

    @Bean
    fun routes() = router {
        GET {
            ServerResponse.ok().body(Mono.just("Let the battle begin!"))
        }

        POST("/**", accept(APPLICATION_JSON)) { request ->
            request.bodyToMono(ArenaUpdate::class.java).flatMap { arenaUpdate ->
                println(arenaUpdate)
                writeCommittedStream?.send(arenaUpdate.arena);
                ServerResponse.ok().body(Mono.just(listOf("R", "L", "T").random()))
            }
        }
    }
}

val projectId: String = ServiceOptions.getDefaultProjectId()
const val datasetName = "snowball"
const val tableName = "events"

var writeCommittedStream: WriteCommittedStream? = null

@Throws(Descriptors.DescriptorValidationException::class, IOException::class, InterruptedException::class)
fun main(args: Array<String>) {
    writeCommittedStream = WriteCommittedStream(projectId, datasetName, tableName)
    runApplication<KotlinApplication>(*args)
}

data class ArenaUpdate(val _links: Links, val arena: Arena)
data class PlayerState(val x: Int, val y: Int, val direction: String, val score: Int, val wasHit: Boolean)
data class Links(val self: Self)
data class Self(val href: String)
data class Arena(val dims: List<Int>, val state: Map<String, PlayerState>)

class WriteCommittedStream(projectId: String?, datasetName: String?, tableName: String?) {
    var jsonStreamWriter: JsonStreamWriter? = null
    fun send(arena: Arena): ApiFuture<AppendRowsResponse> {
        val now = Instant.now()
        val jsonArray = JSONArray()
        arena.state.forEach { url, playerState ->
            val jsonObject = JSONObject()
            jsonObject.put("x", playerState.x)
            jsonObject.put("y", playerState.y)
            jsonObject.put("direction", playerState.direction)
            jsonObject.put("wasHit", playerState.wasHit)
            jsonObject.put("score", playerState.score)
            jsonObject.put("player", url)
            jsonObject.put("timestamp", now.epochSecond * 1000 * 1000)
            jsonArray.put(jsonObject)
        }
        return jsonStreamWriter!!.append(jsonArray)
    }

    init {
        BigQueryWriteClient.create().use { client ->
            val stream: WriteStream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build()
            val parentTable: TableName = TableName.of(projectId, datasetName, tableName)
            val createWriteStreamRequest: CreateWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
                .setParent(parentTable.toString())
                .setWriteStream(stream)
                .build()
            val writeStream: WriteStream = client.createWriteStream(createWriteStreamRequest)
            jsonStreamWriter = JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema()).build()
        }
    }
}