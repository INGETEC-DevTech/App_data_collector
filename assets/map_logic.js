// On initialise les variables globales
var leafletMapInstance = null;
var selectionContour = null;

(function() {
    var pyHandlerIsReady = false;
    var leafletMapInstance = null;
    var lastDrawnRectangle = null;

    // Cache DÉFINITIVEMENT les boutons natifs de Leaflet (Edit + Poubelle)
    function masquerBarreEditionNative() {
        var btnEdit = document.querySelector('.leaflet-draw-edit-edit');
        var btnRemove = document.querySelector('.leaflet-draw-edit-remove');
        if (btnEdit) { btnEdit.style.display = 'none'; }
        if (btnRemove) { btnRemove.style.display = 'none'; }
    }

    // Fonction pour supprimer la sélection
    window.clearMap = function() {
        if (leafletMapInstance) {
            if (window.selectionContour) { leafletMapInstance.removeLayer(window.selectionContour); window.selectionContour = null; }
            if (lastDrawnRectangle) { 
                try {
                    if (lastDrawnRectangle.editing) lastDrawnRectangle.editing.disable();
                    leafletMapInstance.removeLayer(lastDrawnRectangle); 
                } catch(e) {}
                lastDrawnRectangle = null; 
            }
            
            // NETTOYAGE ICI : Plus besoin de vider les toolbars Leaflet, on ne les utilise plus
            
            masquerBarreEditionNative(); // On remplace l'ancien 'verrouillerBoutonsEdition'
        }
    };

    // NOUVEAU : Fonction pour piloter l'édition depuis PyQt
    window.toggleRectangleEdit = function(isEditing) {
        masquerBarreEditionNative(); // Sécurité
        if (lastDrawnRectangle && lastDrawnRectangle.editing) {
            if (isEditing) {
                lastDrawnRectangle.editing.enable();
            } else {
                lastDrawnRectangle.editing.disable();
            }
        }
    };

    window.drawTerritory = function(featureData, isPrecise, shouldZoom) {
        if (!leafletMapInstance) {
            for (var k in window) { if (window[k] instanceof L.Map) { leafletMapInstance = window[k]; break; } }
        }
        
        if (leafletMapInstance) {
            masquerBarreEditionNative(); // On cache les boutons natifs

            if (window.selectionContour) { leafletMapInstance.removeLayer(window.selectionContour); window.selectionContour = null; }
            if (lastDrawnRectangle) { 
                try { 
                    if(lastDrawnRectangle.editing) lastDrawnRectangle.editing.disable(); // Stop edition avant suppression
                    leafletMapInstance.removeLayer(lastDrawnRectangle); 
                } catch(err){}
            }
            
            lastDrawnRectangle = null;

            if (!isPrecise) {
                var tempLayer = L.geoJSON(featureData);
                var bounds = tempLayer.getBounds();

                // Fond en pointillé
                window.selectionContour = L.geoJSON(featureData, {
                    style: { color: "#2c3e50", weight: 2, opacity: 0.6, fillOpacity: 0, dashArray: '5, 10' },
                    interactive: false
                }).addTo(leafletMapInstance);

                // Création et ajout DIRECT du rectangle
                lastDrawnRectangle = L.rectangle(bounds, {
                    color: "#3498db", weight: 2, fillOpacity: 0.3, fillColor: "#3498db"
                }).addTo(leafletMapInstance);

                // TRÈS IMPORTANT : Écouteur unique sur le rectangle
                lastDrawnRectangle.on('edit', function() {
                    if (window.pyHandler) { 
                        window.pyHandler.receive_bbox(JSON.stringify(lastDrawnRectangle.toGeoJSON())); 
                    }
                });

                // On envoie la géométrie initiale à Python
                if (window.pyHandler) { 
                    window.pyHandler.receive_bbox(JSON.stringify(lastDrawnRectangle.toGeoJSON())); 
                }

                if (shouldZoom) { leafletMapInstance.fitBounds(bounds, {padding: [30, 30]}); }
                
            } else {
                window.selectionContour = L.geoJSON(featureData, {
                    style: { color: "#2c3e50", weight: 3, opacity: 0.8, fillColor: "#3498db", fillOpacity: 0.2 },
                    interactive: false
                }).addTo(leafletMapInstance);
                
                if (shouldZoom) { leafletMapInstance.fitBounds(window.selectionContour.getBounds(), {padding: [30, 30]}); }
            }
        }
    };
    
    function findMapAndAttachDrawEvents() {
        if (!leafletMapInstance) {
            for (var k in window) { if (window[k] instanceof L.Map) { leafletMapInstance = window[k]; break; } }
        }
        if (leafletMapInstance && !leafletMapInstance._draw_events_attached) {
            
            // On garde UNIQUEMENT 'draw:created' au cas où l'utilisateur dessine un rectangle à la main 
            // via le bouton carré de la barre d'outil à gauche (s'il n'est pas caché).
            leafletMapInstance.on('draw:created', function(e) {
                if (e.layerType === 'rectangle') {
                    if (lastDrawnRectangle) { 
                        try { 
                            if(lastDrawnRectangle.editing) lastDrawnRectangle.editing.disable();
                            leafletMapInstance.removeLayer(lastDrawnRectangle); 
                        } catch(err){} 
                    }
                    leafletMapInstance.addLayer(e.layer);
                    lastDrawnRectangle = e.layer;
                    
                    // On attache l'écouteur à ce nouveau rectangle manuel
                    lastDrawnRectangle.on('edit', function() {
                        if (window.pyHandler) { window.pyHandler.receive_bbox(JSON.stringify(lastDrawnRectangle.toGeoJSON())); }
                    });

                    if (window.pyHandler) { window.pyHandler.receive_bbox(JSON.stringify(e.layer.toGeoJSON())); }
                    
                    // On force le désactivage de l'édition native de Leaflet.Draw pour que PyQt prenne le relais
                    masquerBarreEditionNative();
                }
            });

            // CLIC SUR LA CARTE POUR VALIDER
            leafletMapInstance.on('click', function(e) {
                // Si on a un rectangle et qu'il est en cours d'édition (les poignées sont visibles)
                if (lastDrawnRectangle && lastDrawnRectangle.editing && lastDrawnRectangle.editing.enabled()) {
                    
                    lastDrawnRectangle.editing.disable(); // On fige le rectangle sur la carte
                    
                    // On prévient Python pour qu'il décoche le bouton vert dans l'interface !
                    if (window.pyHandler && window.pyHandler.finish_edition_from_js) {
                        window.pyHandler.finish_edition_from_js();
                    }
                }
            });

            leafletMapInstance._draw_events_attached = true;
            masquerBarreEditionNative(); // On remplace l'ancien 'verrouillerBoutonsEdition'
        }
    } 
    
    function checkReadyState() {
        if (!pyHandlerIsReady && typeof qt !== 'undefined') {
            new QWebChannel(qt.webChannelTransport, function(channel) {
                window.pyHandler = channel.objects.pyHandler;
                pyHandlerIsReady = true;
            });
        }
        if (typeof L !== 'undefined' && L.map) { findMapAndAttachDrawEvents(); }
    }

    setInterval(checkReadyState, 500);
})();