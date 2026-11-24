package com.example.taskflow;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/tickets")
public class TicketController {

    public record CreateTicket(String title, String description, String severity, String incidentId) {}
    public record CommentBody(String body) {}
    public record Comment(String author, String body, Instant at) {}
    public record Ticket(String id, String title, String description, String severity, String incidentId,
                         String status, String createdBy, Instant createdAt, List<Comment> comments) {}

    private final Map<String, Ticket> tickets = new ConcurrentHashMap<>();
    private final AtomicLong seq = new AtomicLong();

    @PostMapping
    @PreAuthorize("hasAuthority('TICKET_CREATE')")
    public ResponseEntity<Ticket> create(@RequestBody CreateTicket req, @AuthenticationPrincipal Jwt jwt) {
        if (req.title() == null || req.title().isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        String id = "TF-" + seq.incrementAndGet();
        Ticket t = new Ticket(id, req.title(), req.description(), req.severity(), req.incidentId(),
                "OPEN", jwt.getSubject(), Instant.now(), new CopyOnWriteArrayList<>());
        tickets.put(id, t);
        return ResponseEntity.status(HttpStatus.CREATED).body(t);
    }

    @GetMapping
    @PreAuthorize("hasAuthority('TICKET_READ')")
    public Collection<Ticket> list() {
        return new ArrayList<>(tickets.values());
    }

    @GetMapping("/{id}")
    @PreAuthorize("hasAuthority('TICKET_READ')")
    public ResponseEntity<Ticket> get(@PathVariable String id) {
        Ticket t = tickets.get(id);
        return t == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(t);
    }

    @PostMapping("/{id}/comments")
    @PreAuthorize("hasAuthority('TICKET_COMMENT')")
    public ResponseEntity<Ticket> comment(@PathVariable String id, @RequestBody CommentBody req,
                                          @AuthenticationPrincipal Jwt jwt) {
        Ticket t = tickets.get(id);
        if (t == null) return ResponseEntity.notFound().build();
        t.comments().add(new Comment(jwt.getSubject(), req.body(), Instant.now()));
        return ResponseEntity.ok(t);
    }

    /** Exists for humans/admins. The copilot's token lacks TICKET_DELETE and its MCP server has no such tool. */
    @DeleteMapping("/{id}")
    @PreAuthorize("hasAuthority('TICKET_DELETE')")
    public ResponseEntity<Void> delete(@PathVariable String id) {
        return tickets.remove(id) == null ? ResponseEntity.notFound().build() : ResponseEntity.noContent().build();
    }
}
